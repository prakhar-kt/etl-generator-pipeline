"""Generate DQ test SQL queries from BL/BR YAML mapping content."""

import re

import yaml


def generate_tests(yaml_content: str, project: str) -> list[dict]:
    """Generate data quality test definitions from a BL/BR YAML mapping.

    Returns list of dicts with keys: name, sql, expected, severity, description
    """
    # Strip Jinja2 {{ }} placeholders before YAML parsing — they break yaml.safe_load
    safe_yaml = re.sub(r'\{\{[^}]*\}\}', 'PLACEHOLDER', yaml_content)
    try:
        mapping = yaml.safe_load(safe_yaml)
    except yaml.YAMLError:
        # Fallback: try to extract table name from raw text
        mapping = None

    if not mapping:
        # Try regex extraction as last resort
        m = re.search(r'target_table_name:\s*["\']?([^"\'\n]+)', yaml_content)
        target_table = m.group(1).strip().strip('"').strip("'").split(".")[-1] if m else ""
        if not target_table:
            m = re.search(r'`[^`]+\.[^`]+\.([^`]+)`', yaml_content)
            target_table = m.group(1) if m else ""
        if not target_table:
            return []
        # Build minimal mapping for test generation
        mapping = {
            "metadata": {"target_table_name": target_table},
            "create_table": "",
        }

    metadata = mapping.get("metadata", {})
    target_table_fq = metadata.get("target_table_name", "")
    # Extract just the table name (last part after dots)
    target_table = target_table_fq.split(".")[-1] if target_table_fq else ""
    if not target_table:
        # Try to extract from create_table SQL
        create_sql = mapping.get("create_table", "")
        m = re.search(r'`[^`]+\.[^`]+\.([^`]+)`', create_sql or "")
        if m:
            target_table = m.group(1)
    if not target_table:
        return []
    # Clean up placeholder artifacts
    target_table = target_table.replace("PLACEHOLDER", "").strip()

    # Extract dataset from create_table or default
    create_sql = mapping.get("create_table", "")
    dataset = "Business_Logic"
    m = re.search(r'`[^`]+\.([^`]+)\.[^`]+`', create_sql)
    if m:
        dataset = m.group(1)

    fq_table = f"`{project}.{dataset}.{target_table}`"

    columns = _extract_columns_from_ddl(create_sql)
    tests = []

    # 1. Schema check — verify expected columns exist (run first)
    if columns:
        expected_cols = ", ".join(f"'{c[0]}'" for c in columns)
        tests.append({
            "name": "schema_check",
            "description": "Verify all expected columns exist in the table",
            "sql": f"""SELECT expected_col
FROM UNNEST([{expected_cols}]) AS expected_col
WHERE expected_col NOT IN (
  SELECT column_name
  FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '{target_table}'
)""",
            "expected": "0_rows",
            "severity": "error",
        })

    # 2. Row count check
    tests.append({
        "name": "row_count_check",
        "description": "Verify table has data",
        "sql": f"SELECT COUNT(*) as cnt FROM {fq_table}",
        "expected": "gt_zero",
        "severity": "error",
    })

    # 3. Duplicate check on ADMIN_COMPOSITEKEY_HASH
    tests.append({
        "name": "duplicate_check",
        "description": "Check for duplicate composite key hashes",
        "sql": f"""SELECT ADMIN_COMPOSITEKEY_HASH, COUNT(*) as cnt
FROM {fq_table}
WHERE ADMIN_ISDELETED = FALSE OR ADMIN_ISDELETED IS NULL
GROUP BY ADMIN_COMPOSITEKEY_HASH
HAVING cnt > 1
LIMIT 5""",
        "expected": "0_rows",
        "severity": "error",
    })

    # 4. Null check
    if columns:
        countif_parts = []
        for col_name, col_type in columns:
            countif_parts.append(f"  COUNTIF({col_name} IS NULL) as {col_name}_nulls")
        null_sql = f"SELECT\n" + ",\n".join(countif_parts) + f"\nFROM {fq_table}"
        tests.append({
            "name": "null_check",
            "description": "Check for NULL values in all columns",
            "sql": null_sql,
            "expected": "all_zero",
            "severity": "warning",
            "_columns": columns,
        })

    # 5. Source coverage
    source_tables = _extract_source_tables(mapping, project)
    if source_tables:
        for src_fq, src_name in source_tables[:2]:
            tests.append({
                "name": f"source_coverage_{src_name}",
                "description": f"Check row coverage from {src_name}",
                "sql": f"""SELECT
  (SELECT COUNT(*) FROM {src_fq}) as source_rows,
  (SELECT COUNT(*) FROM {fq_table}) as target_rows""",
                "expected": "informational",
                "severity": "warning",
            })

    return tests


def _extract_columns_from_ddl(create_sql: str) -> list[tuple[str, str]]:
    """Extract (column_name, type) pairs from CREATE TABLE DDL."""
    if not create_sql:
        return []
    # Find content between first ( and last )
    m = re.search(r'\(\s*\n(.*?)\n\s*\)', create_sql, re.DOTALL)
    if not m:
        return []
    body = m.group(1)
    columns = []
    for line in body.strip().split('\n'):
        line = line.strip().rstrip(',')
        if not line or line.startswith('--'):
            continue
        # Match: COLUMN_NAME    TYPE [OPTIONS(...)]
        cm = re.match(r'(\w+)\s+(STRING|INT64|FLOAT64|NUMERIC|BOOL|DATE|TIMESTAMP|BYTES|BIGNUMERIC)', line)
        if cm:
            columns.append((cm.group(1), cm.group(2)))
    return columns


def _extract_source_tables(mapping: dict, project: str) -> list[tuple[str, str]]:
    """Extract fully-qualified source table references from YAML."""
    source_names = mapping.get("metadata", {}).get("source_table_names", "")
    if not source_names:
        return []

    # Handle both list and comma-separated string formats
    if isinstance(source_names, list):
        parts_list = source_names
    else:
        parts_list = source_names.split(",")

    tables = []
    for part in parts_list:
        part = part.strip().strip("`").strip("'").strip('"')
        if not part:
            continue
        # Extract table name (last segment after dots)
        segments = part.split(".")
        table_name = segments[-1].strip()
        if not table_name:
            continue
        # Determine dataset
        if table_name.lower().startswith("cdl_"):
            ds = "CDL_NovaStar"
        elif table_name.lower().startswith("src_") or table_name.lower().startswith("raw_"):
            ds = "Src_NovaStar"
        else:
            ds = "Business_Logic"
        fq = f"`{project}.{ds}.{table_name}`"
        tables.append((fq, table_name))
    return tables


def evaluate_test_result(test: dict, rows: list) -> dict:
    """Evaluate test results against expected outcome.

    Returns dict with: name, status (pass/fail), detail
    """
    name = test["name"]
    expected = test["expected"]
    severity = test["severity"]

    if expected == "0_rows":
        if len(rows) == 0:
            return {"name": name, "status": "pass", "detail": "No violations found", "severity": severity}
        else:
            return {"name": name, "status": "fail", "detail": f"Found {len(rows)} violation(s)", "severity": severity}

    elif expected == "gt_zero":
        if rows and rows[0].get("cnt", 0) > 0:
            return {"name": name, "status": "pass", "detail": f"{rows[0]['cnt']:,} rows", "severity": severity}
        else:
            return {"name": name, "status": "fail", "detail": "Table is empty", "severity": severity}

    elif expected == "all_zero":
        # Check that all *_nulls columns are 0
        if rows:
            row = rows[0]
            failures = {k: v for k, v in row.items() if k.endswith("_nulls") and v > 0}
            if failures:
                detail = ", ".join(f"{k}={v}" for k, v in list(failures.items())[:5])
                return {"name": name, "status": "fail", "detail": f"NULLs found: {detail}", "severity": severity}
        return {"name": name, "status": "pass", "detail": "No NULLs found", "severity": severity}

    elif expected == "informational":
        detail = ", ".join(f"{k}={v}" for k, v in (rows[0].items() if rows else {}.items()))
        return {"name": name, "status": "pass", "detail": detail or "OK", "severity": severity}

    return {"name": name, "status": "pass", "detail": "OK", "severity": severity}
