"""Shared SQL utility functions for fixing LLM-generated BigQuery SQL."""

import re


def extract_dataset_name(create_sql: str) -> str:
    """Extract dataset name from CREATE TABLE SQL: `project.dataset.table`."""
    m = re.search(r'`[^`]+\.([^`]+)\.[^`]+`', create_sql)
    return m.group(1) if m else "Business_Logic"


def replace_placeholders(sql: str, project: str, dataset_name: str = "Business_Logic") -> str:
    """Replace Jinja2 placeholders and fix common LLM-generated placeholder text."""
    # Step 1: Replace "GBQ Project.Dataset." with just dataset_name FIRST
    sql = re.sub(r'GBQ Project\.Dataset\.', f'{dataset_name}.', sql, flags=re.IGNORECASE)
    sql = re.sub(r'GBQ Project\.', '', sql, flags=re.IGNORECASE)

    # Step 2: Replace Jinja2 template vars
    sql = sql.replace("{{ target_project }}", project)
    sql = sql.replace("{{ source_projects[0] }}", project)
    sql = sql.replace("{{ source_projects[1] }}", project)
    sql = sql.replace("{{ source_projects[2] }}", project)
    sql = sql.replace("{{ process_id }}", "web-ui-exec")
    sql = sql.replace("{{ incremental_value }}", "1900-01-01")
    sql = sql.replace("{{ max_date }}", "1900-01-01 00:00:00")
    # For quoted contexts like '{{ var }}', replace the whole quoted placeholder
    sql = re.sub(r"'\{\{[^}]*\}\}'", "'1900-01-01'", sql)
    # For unquoted remaining placeholders, just remove the braces
    sql = re.sub(r'\{\{[^}]*\}\}', '0', sql)

    # Step 3: Rename bl_dim_* to cdl_dim_* (LLM invents bl_dim tables that don't exist)
    sql = re.sub(r'\bbl_dim_', 'cdl_dim_', sql, flags=re.IGNORECASE)

    # Step 3b: Route tables to correct datasets based on table name prefix
    esc_project = re.escape(project)
    sql = re.sub(
        rf'`{esc_project}\.[^`]*?\.(cdl_|src_|raw_)',
        lambda m: f'`{project}.{"CDL_NovaStar" if m.group(1) == "cdl_" else "Src_NovaStar"}.{m.group(1)}',
        sql, flags=re.IGNORECASE
    )

    # Step 4: Fix common LLM column name mistakes
    sql = re.sub(r'\bLAST_MODIFY_DATE\b', 'CDL_LOAD_DATE', sql)
    sql = re.sub(r'\bLOAD_DATE\b', 'CDL_LOAD_DATE', sql)

    # Fix ADMIN_ROW_HASH: TO_JSON_STRING(src/SOURCE) doesn't work in BQ MERGE
    sql = re.sub(r'FARM_FINGERPRINT\(TO_JSON_STRING\([^)]*\)\)', '0', sql)

    # Fix MERGE USING <bare_cte_name> — BQ requires a subquery, not a bare CTE reference
    # USING some_cte AS SOURCE → USING (SELECT * FROM some_cte) AS SOURCE
    sql = re.sub(
        r'USING\s+([a-zA-Z_]\w*)\s+AS\s+SOURCE\b',
        r'USING (SELECT * FROM \1) AS SOURCE',
        sql, flags=re.IGNORECASE
    )

    # Fix reserved words used as CTE/alias names — BQ treats them as table refs
    reserved_aliases = {
        'src': 'src_cte',
        'final': 'final_cte',
        'source': 'source_cte',
        'result': 'result_cte',
        'data': 'data_cte',
    }
    for bad, good in reserved_aliases.items():
        # Replace backtick-quoted versions
        sql = sql.replace(f'`{bad}`', good)
        # Replace CTE definition: bad AS ( → good AS (
        sql = re.sub(rf'\b{bad}\s+AS\s*\(', f'{good} AS (', sql, flags=re.IGNORECASE)
        # Replace CTE definition after ): ) bad AS → ) good AS
        sql = re.sub(rf'\)\s*,?\s*{bad}\s+AS\b', f'), {good} AS', sql, flags=re.IGNORECASE)
        # Replace references: FROM final → FROM final_cte, JOIN final → JOIN final_cte
        sql = re.sub(rf'\bFROM\s+{bad}\b', f'FROM {good}', sql, flags=re.IGNORECASE)
        sql = re.sub(rf'\bJOIN\s+{bad}\b', f'JOIN {good}', sql, flags=re.IGNORECASE)
        # Replace alias references: final.column → final_cte.column
        sql = re.sub(rf'\b{bad}\.(\w)', rf'{good}.\1', sql)

    return sql


def cleanup_sql(sql: str) -> str:
    """Fix common LLM-generated SQL issues before execution."""
    # Remove AS aliases inside the VALUES() clause of MERGE INSERT.
    upper = sql.upper()
    values_idx = upper.rfind('VALUES')
    if values_idx != -1:
        before = sql[:values_idx]
        after = sql[values_idx:]
        after = re.sub(r'\)\s+AS\s+[A-Za-z_][A-Za-z0-9_]*', ')', after)
        after = re.sub(r"'\s+AS\s+[A-Za-z_][A-Za-z0-9_]*", "'", after)
        after = re.sub(r',\s*\n\s*AS\s+[A-Za-z_][A-Za-z0-9_]*', '', after)
        after = re.sub(
            r'(?<!CAST\()(?<!CAST )\b(FALSE|TRUE|CURRENT_TIMESTAMP\(\))\s+AS\s+[A-Za-z_][A-Za-z0-9_]*',
            r'\1', after
        )
        sql = before + after
    return sql


def fix_type_mismatches(merge_sql: str, create_sql: str) -> str:
    """Fix FLOAT64/NUMERIC type mismatches in MERGE SQL based on DDL column types.

    When a NUMERIC column gets assigned a FLOAT64 expression (division, ROUND, etc.),
    wrap the expression with CAST(... AS NUMERIC).
    """
    if not create_sql or not merge_sql:
        return merge_sql

    # Extract NUMERIC columns from DDL
    numeric_cols = set()
    for line in create_sql.split('\n'):
        line = line.strip().rstrip(',')
        m = re.match(r'(\w+)\s+NUMERIC', line)
        if m:
            numeric_cols.add(m.group(1).upper())

    if not numeric_cols:
        return merge_sql

    # Functions that return FLOAT64 in BigQuery
    float_funcs = r'(?:SAFE_DIVIDE|ROUND|IEEE_DIVIDE|SQRT|LOG|LN|EXP|POW|ACOS|ASIN|ATAN)'

    for col in numeric_cols:
        # Fix UPDATE SET: TARGET.COL = float_expr → TARGET.COL = CAST(float_expr AS NUMERIC)
        pattern = rf'(TARGET\.{col}\s*=\s*)(?!CAST\()(?!SOURCE\.)({float_funcs}\s*\()'
        def wrap_update(m):
            prefix = m.group(1)
            expr_start = m.group(2)
            # Find the full expression by matching balanced parens
            return prefix + 'CAST(' + expr_start
        # Simple approach: if we see TARGET.COL = SAFE_DIVIDE/ROUND, insert CAST
        merge_sql = re.sub(
            rf'(TARGET\.{col}\s*=\s*)({float_funcs}\s*\([^;\n]*?)(\s*,\s*\n|\s*$)',
            rf'\1CAST(\2 AS NUMERIC)\3',
            merge_sql, flags=re.IGNORECASE | re.MULTILINE
        )

        # Fix SELECT: float_expr AS COL → CAST(float_expr AS NUMERIC) AS COL
        # Match function call with possible nested parens, then AS COL
        merge_sql = re.sub(
            rf'(?<!CAST\()({float_funcs}\s*\((?:[^()]*\([^()]*\))*[^()]*\)(?:\s*\*\s*[\d.]+)?)\s+AS\s+{col}\b',
            rf'CAST(\1 AS NUMERIC) AS {col}',
            merge_sql, flags=re.IGNORECASE
        )

        # Also fix: plain arithmetic like (A - B) * 100.0 AS COL
        merge_sql = re.sub(
            rf'(?<!CAST\()((?:COALESCE\s*\([^)]+\)\s*[-+*/]\s*)+[^\n,]+?(?:\*\s*[\d.]+))\s+AS\s+{col}\b',
            rf'CAST(\1 AS NUMERIC) AS {col}',
            merge_sql, flags=re.IGNORECASE
        )

    return merge_sql


def prepare_merge_sql(sql: str) -> str:
    """Strip any stray SQL before the MERGE/DELETE/INSERT keyword."""
    m = re.search(r'(?:^|\n)\s*(MERGE\s+INTO|DELETE\s+FROM|INSERT\s+INTO)', sql, re.IGNORECASE)
    if m:
        sql = sql[m.start(1):]
    return sql


def prepare_sql(sql: str, project: str, dataset_name: str = "Business_Logic") -> str:
    """Full SQL preparation: placeholders + cleanup."""
    return cleanup_sql(replace_placeholders(sql, project, dataset_name))


def ensure_datasets(client, project: str, dataset_name: str = "Business_Logic"):
    """Ensure all required BQ datasets exist."""
    from google.cloud import bigquery as bq
    bq_location = "us-central1"
    required = {dataset_name, "Business_Logic", "CDL_NovaStar", "Src_NovaStar"}
    created = []
    for ds_name in required:
        try:
            dataset_ref = bq.DatasetReference(project, ds_name)
            client.get_dataset(dataset_ref)
        except Exception:
            try:
                dataset = bq.Dataset(dataset_ref)
                dataset.location = bq_location
                client.create_dataset(dataset)
                created.append(ds_name)
            except Exception:
                pass
    return created
