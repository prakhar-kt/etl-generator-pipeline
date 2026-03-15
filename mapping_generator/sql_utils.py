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

    # Step 3: Route tables to correct datasets based on table name prefix
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
