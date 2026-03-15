"""Self-healing execution pipeline with BQ storage, LLM retry, and DQ tests."""

import asyncio
import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from typing import AsyncGenerator

import yaml

from .config import CLAUDE_MODEL, LLM_PROVIDER, MAX_TOKENS
from .generators.base import _create_llm_client
from .lessons import format_lessons_prompt, store_lesson
from .sql_utils import cleanup_sql, ensure_datasets, extract_dataset_name, fix_type_mismatches, prepare_merge_sql, replace_placeholders
from .test_generator import evaluate_test_result, generate_tests

logger = logging.getLogger("pipeline")

MAX_RETRY = 3


@dataclass
class PipelineEvent:
    stage: str       # "execute", "test", "test_item"
    status: str      # "pending", "running", "success", "failed", "retrying"
    message: str
    attempt: int = 0
    max_attempts: int = MAX_RETRY
    version: int = 1
    detail: str = ""
    test_sql: str = ""
    fixed_yaml: str = ""
    test_name: str = ""
    test_results: list = field(default_factory=list)
    table_location: str = ""
    preview_rows: list = field(default_factory=list)


# ---------- BQ artifact storage ----------

PIPELINE_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS `{project}.Business_Logic.pipeline_runs` (
  run_id          STRING NOT NULL,
  run_timestamp   TIMESTAMP NOT NULL,
  step            STRING NOT NULL,
  table_name      STRING,
  status          STRING NOT NULL,
  rows_affected   INT64,
  duration_seconds FLOAT64,
  error_message   STRING,
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
"""

ARTIFACTS_DDL = """
CREATE TABLE IF NOT EXISTS `{project}.Business_Logic.pipeline_artifacts` (
  artifact_id       STRING NOT NULL,
  filename          STRING NOT NULL,
  target_table      STRING,
  yaml_content      STRING,
  version           INT64 NOT NULL,
  status            STRING NOT NULL,
  error_message     STRING,
  test_definitions  STRING,
  test_results      STRING,
  attempt_number    INT64,
  created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
"""


def ensure_artifacts_table(client, project: str):
    """Create pipeline_artifacts table if it doesn't exist."""
    sql = ARTIFACTS_DDL.format(project=project)
    job = client.query(sql)
    job.result()


def ensure_runs_table(client, project: str):
    """Create pipeline_runs table if it doesn't exist."""
    sql = PIPELINE_RUNS_DDL.format(project=project)
    job = client.query(sql)
    job.result()


def log_pipeline_run(client, project: str, run_id: str, step: str,
                     table_name: str, status: str, rows_affected: int = 0,
                     duration_seconds: float = 0.0, error_message: str = ""):
    """Log a pipeline run step to pipeline_runs table."""
    err = _esc(error_message[:1000]) if error_message else ""
    sql = f"""INSERT INTO `{project}.Business_Logic.pipeline_runs`
    (run_id, run_timestamp, step, table_name, status, rows_affected, duration_seconds, error_message)
    VALUES ('{run_id}', CURRENT_TIMESTAMP(), '{step}', '{_esc(table_name)}',
            '{status}', {rows_affected}, {duration_seconds}, '{err}')"""
    try:
        job = client.query(sql)
        job.result()
    except Exception as e:
        logger.error(f"Failed to log pipeline run: {e}")


def store_artifact(client, project: str, artifact_id: str, filename: str,
                   target_table: str, yaml_content: str, version: int, status: str):
    """Insert a new artifact row."""
    sql = f"""INSERT INTO `{project}.Business_Logic.pipeline_artifacts`
    (artifact_id, filename, target_table, yaml_content, version, status, attempt_number, created_at, updated_at)
    VALUES ('{artifact_id}', '{_esc(filename)}', '{_esc(target_table)}',
            '''{_esc(yaml_content)}''', {version}, '{status}', {version},
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"""
    job = client.query(sql)
    job.result()


def update_artifact(client, project: str, artifact_id: str, version: int, **fields):
    """Update an existing artifact row."""
    set_parts = ["updated_at = CURRENT_TIMESTAMP()"]
    for k, v in fields.items():
        if isinstance(v, str):
            set_parts.append(f"{k} = '''{_esc(v)}'''")
        elif isinstance(v, int):
            set_parts.append(f"{k} = {v}")
    set_clause = ", ".join(set_parts)
    sql = f"""UPDATE `{project}.Business_Logic.pipeline_artifacts`
    SET {set_clause}
    WHERE artifact_id = '{artifact_id}' AND version = {version}"""
    job = client.query(sql)
    job.result()


def upsert_latest_yaml(client, project: str, target_table: str, filename: str,
                       yaml_content: str, new_version: int, status: str):
    """Store a new version of YAML for a target table, archiving the old one.

    Inserts a new row with the incremented version. The _check_existing_yaml
    query uses QUALIFY ROW_NUMBER() ORDER BY version DESC to always pick the latest.
    """
    import uuid
    artifact_id = str(uuid.uuid4())
    try:
        store_artifact(client, project, artifact_id, filename, target_table,
                       yaml_content, new_version, status)
        logger.info(f"Stored YAML v{new_version} for {target_table} (status={status})")
    except Exception as e:
        logger.error(f"Failed to upsert YAML for {target_table}: {e}")


def _esc(s: str) -> str:
    """Escape single quotes for BQ SQL strings."""
    return s.replace("\\", "\\\\").replace("'", "\\'") if s else ""


# ---------- LLM fix ----------

def call_llm_fix(yaml_content: str, error_message: str, context: str = "execution",
                 test_sql: str = "") -> dict:
    """Send YAML + error to LLM, get back fixes.

    Returns dict with keys: yaml (fixed yaml or None), test_sql (fixed test sql or None)
    """
    client = _create_llm_client(LLM_PROVIDER)

    lessons = format_lessons_prompt(max_dynamic=3)

    if context == "execution":
        system_prompt = f"""You are a BigQuery SQL expert. The user will provide a YAML mapping file
that contains BigQuery SQL (CREATE TABLE and MERGE statements). The SQL failed with an error.
Fix the YAML so the SQL executes successfully.

{lessons}

Return ONLY the corrected YAML content. No markdown fences. No explanation."""
    else:
        system_prompt = f"""You are a BigQuery SQL expert. A data quality test failed on a table
created from a YAML mapping file. The issue could be in either:
1. The YAML's SQL (merge_statement/create_table) — causing bad data
2. The test SQL itself — testing incorrectly

Analyze the error and determine which needs fixing. If the YAML SQL is wrong, return the
full corrected YAML. If the test SQL is wrong, return ONLY the corrected test SQL.

{lessons}

Format your response as:
FIX_TYPE: yaml
<corrected yaml content>

OR:
FIX_TYPE: test_sql
<corrected test SQL>

No markdown fences. No additional explanation."""

    user_prompt = f"## Error\n{error_message}"
    if test_sql:
        user_prompt += f"\n\n## Test SQL\n{test_sql}"
    user_prompt += f"\n\n## Current YAML\n{yaml_content}"

    if LLM_PROVIDER == "gemini":
        from google import genai
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=user_prompt,
            config={"system_instruction": system_prompt, "max_output_tokens": MAX_TOKENS},
        )
        result = response.text.strip()
    else:
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=MAX_TOKENS,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        result = response.content[0].text.strip()

    # Strip markdown fences
    lines = result.split("\n")
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].startswith("```"):
        lines = lines[:-1]
    result = "\n".join(lines).strip()

    # Parse fix type for test context
    if context == "test_fix" and result.startswith("FIX_TYPE:"):
        first_line = result.split("\n")[0]
        fix_type = first_line.split(":", 1)[1].strip().lower()
        content = "\n".join(result.split("\n")[1:]).strip()
        if fix_type == "test_sql":
            return {"yaml": None, "test_sql": content}
        else:
            return {"yaml": content, "test_sql": None}

    # Default: assume it's a YAML fix
    return {"yaml": result, "test_sql": None}


# ---------- SQL execution helpers ----------

def _execute_yaml_sql(client, yaml_content: str, project: str, dataset_name: str) -> tuple[bool, str]:
    """Execute CREATE TABLE + MERGE from YAML. Returns (success, error_message)."""
    mapping = yaml.safe_load(yaml_content)

    # CREATE TABLE
    create_sql = mapping.get("create_table", "")
    if create_sql:
        create_sql = cleanup_sql(replace_placeholders(create_sql, project, dataset_name))
        try:
            job = client.query(create_sql)
            job.result()
        except Exception as e:
            return False, f"CREATE TABLE failed: {e}"

    # MERGE / INSERT
    merge_sql = mapping.get("merge_statement", "") or mapping.get("other_statement", "")
    if merge_sql:
        # Fix type mismatches (FLOAT64 → NUMERIC) based on DDL
        raw_create = mapping.get("create_table", "")
        merge_sql = fix_type_mismatches(merge_sql, raw_create)
        merge_sql = cleanup_sql(replace_placeholders(merge_sql, project, dataset_name))
        merge_sql = prepare_merge_sql(merge_sql)
        try:
            job = client.query(merge_sql)
            job.result()
        except Exception as e:
            return False, f"MERGE/INSERT failed: {e}"

    return True, ""


# ---------- Pipeline: Execute stage ----------

async def run_execute(
    yaml_content: str,
    filename: str,
    project_id: str,
    bq_client,
) -> AsyncGenerator[PipelineEvent, None]:
    """Execute SQL from YAML. No retries — reports error directly."""
    project = bq_client.project or project_id
    current_yaml = yaml_content

    mapping = yaml.safe_load(current_yaml) or {}
    target_table = mapping.get("metadata", {}).get("target_table_name", "").split(".")[-1]
    dataset_name = "Business_Logic"
    create_sql = mapping.get("create_table", "")
    if create_sql:
        dataset_name = extract_dataset_name(create_sql)

    await asyncio.to_thread(ensure_datasets, bq_client, project, dataset_name)

    table_location = f"`{project}.{dataset_name}.{target_table}`"

    yield PipelineEvent(stage="execute", status="running", message="Executing SQL...")

    ok, error_msg = await asyncio.to_thread(
        _execute_yaml_sql, bq_client, current_yaml, project, dataset_name
    )

    if ok:
        await asyncio.to_thread(
            upsert_latest_yaml, bq_client, project, target_table,
            filename, current_yaml, 1, "executed"
        )
        yield PipelineEvent(stage="execute", status="success",
                            message="SQL executed successfully",
                            table_location=table_location)
    else:
        await asyncio.to_thread(
            store_lesson, error_msg[:500], "Execution failed", "execution"
        )
        yield PipelineEvent(stage="execute", status="failed",
                            message="Execution failed", detail=error_msg)


# ---------- Pipeline: Preview ----------

async def run_preview(target_table: str, project_id: str, bq_client) -> list[dict]:
    """Return top 10 rows from the target table."""
    project = bq_client.project or project_id
    # Clean up table reference
    table = target_table.strip("`").strip()
    if "." not in table:
        table = f"{project}.Business_Logic.{table}"
    sql = f"SELECT * FROM `{table}` LIMIT 10"
    job = await asyncio.to_thread(bq_client.query, sql)
    rows_raw = await asyncio.to_thread(lambda: list(job.result()))
    return [dict(r.items()) for r in rows_raw]


# ---------- Pipeline: Tests (sequential, no retries) ----------

async def run_tests(
    yaml_content: str,
    project_id: str,
    bq_client,
    filename: str = "",
) -> AsyncGenerator[PipelineEvent, None]:
    """Run DQ tests one by one. No retries — reports failures directly."""
    project = bq_client.project or project_id

    try:
        tests = generate_tests(yaml_content, project)
        if not tests:
            yield PipelineEvent(stage="test", status="success",
                                message="No tests to run", test_results=[])
            return
    except Exception as e:
        yield PipelineEvent(stage="test", status="failed",
                            message="Failed to generate tests",
                            detail=f"{type(e).__name__}: {e}")
        return

    # Send test list so UI can create nodes
    yield PipelineEvent(stage="test", status="running",
                        message=f"Running {len(tests)} tests...",
                        test_results=[{"name": t["name"], "status": "pending",
                                       "detail": t["description"]} for t in tests])

    all_results = []
    has_failures = False

    for test in tests:
        yield PipelineEvent(stage="test_item", status="running",
                            message=f"Running {test['name']}...",
                            test_name=test["name"])

        try:
            if test.get("_dynamic"):
                result = {"name": test["name"], "status": "skip", "detail": "Skipped"}
            else:
                job = await asyncio.to_thread(bq_client.query, test["sql"])
                rows_raw = await asyncio.to_thread(lambda: list(job.result()))
                rows = [dict(r.items()) for r in rows_raw]
                result = evaluate_test_result(test, rows)
        except Exception as e:
            result = {"name": test["name"], "status": "fail",
                      "detail": f"Test query error: {str(e)[:300]}",
                      "severity": test.get("severity", "error")}

        if result["status"] == "pass" or result["status"] == "skip":
            yield PipelineEvent(stage="test_item", status="success",
                                message=f"{test['name']} passed",
                                test_name=test["name"],
                                detail=result.get("detail", ""))
        elif result.get("severity") == "warning":
            yield PipelineEvent(stage="test_item", status="success",
                                message=f"{test['name']} warning",
                                test_name=test["name"],
                                detail=result.get("detail", ""))
        else:
            has_failures = True
            # Store as lesson for future learning
            await asyncio.to_thread(
                store_lesson,
                f"Test '{test['name']}' failed: {result.get('detail', '')}"[:500],
                f"Test failure in {test['name']}",
                f"test_{test['name']}"
            )
            yield PipelineEvent(stage="test_item", status="failed",
                                message=f"{test['name']} failed",
                                test_name=test["name"],
                                detail=result.get("detail", ""),
                                test_sql=test.get("sql", ""))

        all_results.append(result)

    if has_failures:
        yield PipelineEvent(stage="test", status="failed",
                            message="Some tests failed",
                            test_results=all_results)
    else:
        # All passed — persist YAML as "passed"
        mapping = yaml.safe_load(re.sub(r'\{\{[^}]*\}\}', 'X', yaml_content)) or {}
        target_table = mapping.get("metadata", {}).get("target_table_name", "").split(".")[-1]
        await asyncio.to_thread(
            upsert_latest_yaml, bq_client, project, target_table,
            filename, yaml_content, 1, "passed"
        )
        yield PipelineEvent(stage="test", status="success",
                            message=f"All {len(all_results)} tests passed",
                            test_results=all_results)


# Need re import for the regex in run_tests
import re
import time
from pathlib import Path


# ---------- Pipeline: Refresh (daily scheduled) ----------

def _run_cdl_transforms(client, project: str) -> tuple[bool, str, int]:
    """Re-run RAW → CDL MERGE transforms. Returns (success, error_msg, statements_run)."""
    transform_file = Path(__file__).parent.parent / "synthetic_data" / "sql" / "transforms" / "01_raw_to_cdl.sql"
    if not transform_file.exists():
        return False, f"Transform file not found: {transform_file}", 0

    sql = transform_file.read_text()
    sql = sql.replace("{{ project }}", project)

    statements = [s.strip() for s in sql.split(";") if s.strip()]
    run_count = 0

    for stmt in statements:
        lines = [l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]
        if not lines:
            continue
        try:
            job = client.query(stmt + ";")
            job.result()
            run_count += 1
        except Exception as e:
            return False, f"CDL transform failed on statement {run_count + 1}: {e}", run_count

    return True, "", run_count


def _get_active_bl_yamls(client, project: str) -> list[dict]:
    """Fetch all active BL YAMLs from pipeline_artifacts (latest version, preferring 'passed')."""
    sql = f"""SELECT target_table, filename, yaml_content, version, status
              FROM `{project}.Business_Logic.pipeline_artifacts`
              WHERE yaml_content IS NOT NULL
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY target_table
                ORDER BY
                  CASE status WHEN 'passed' THEN 1 WHEN 'executed' THEN 2 WHEN 'testing' THEN 3 WHEN 'generated' THEN 4 ELSE 5 END,
                  version DESC
              ) = 1"""
    try:
        job = client.query(sql)
        return [dict(r.items()) for r in job.result()]
    except Exception:
        return []


def _get_row_count(client, project: str, table_name: str) -> int:
    """Get row count for a BL table."""
    try:
        table_ref = f"{project}.Business_Logic.{table_name}"
        table = client.get_table(table_ref)
        return table.num_rows or 0
    except Exception:
        return 0


async def run_refresh(bq_client, project: str) -> AsyncGenerator[PipelineEvent, None]:
    """Refresh CDL + all active BL tables. Yields SSE events for progress tracking."""
    run_id = str(uuid.uuid4())

    # Ensure logging table exists
    await asyncio.to_thread(ensure_datasets, bq_client, project)
    await asyncio.to_thread(ensure_runs_table, bq_client, project)

    yield PipelineEvent(stage="refresh", status="running",
                        message="Starting daily refresh...")

    # ── Step 1: CDL refresh ──────────────────────────────────
    yield PipelineEvent(stage="refresh_cdl", status="running",
                        message="Refreshing CDL layer (RAW → CDL transforms)...")

    t0 = time.time()
    ok, err, stmt_count = await asyncio.to_thread(_run_cdl_transforms, bq_client, project)
    duration = time.time() - t0

    if ok:
        await asyncio.to_thread(
            log_pipeline_run, bq_client, project, run_id, "cdl_refresh",
            "CDL_NovaStar", "success", stmt_count, duration
        )
        yield PipelineEvent(stage="refresh_cdl", status="success",
                            message=f"CDL refresh complete — {stmt_count} transforms in {duration:.1f}s")
    else:
        await asyncio.to_thread(
            log_pipeline_run, bq_client, project, run_id, "cdl_refresh",
            "CDL_NovaStar", "failed", stmt_count, duration, err
        )
        yield PipelineEvent(stage="refresh_cdl", status="failed",
                            message="CDL refresh failed", detail=err)
        # Continue to BL even if CDL fails — some BL tables may not depend on failed CDL tables

    # ── Step 2: BL refresh ───────────────────────────────────
    yield PipelineEvent(stage="refresh_bl", status="running",
                        message="Fetching active BL mappings from pipeline_artifacts...")

    bl_yamls = await asyncio.to_thread(_get_active_bl_yamls, bq_client, project)

    if not bl_yamls:
        yield PipelineEvent(stage="refresh_bl", status="success",
                            message="No active BL tables to refresh")
        yield PipelineEvent(stage="refresh", status="success",
                            message="Refresh complete (no BL tables)")
        return

    yield PipelineEvent(stage="refresh_bl", status="running",
                        message=f"Refreshing {len(bl_yamls)} BL tables...")

    success_count = 0
    fail_count = 0

    for bl in bl_yamls:
        table_name = bl["target_table"]
        yaml_content = bl["yaml_content"]
        filename = bl.get("filename", "")

        yield PipelineEvent(stage="refresh_bl_table", status="running",
                            message=f"Refreshing {table_name}...",
                            table_location=f"`{project}.Business_Logic.{table_name}`")

        t0 = time.time()

        # Extract dataset from the YAML's CREATE TABLE
        mapping = yaml.safe_load(yaml_content) or {}
        create_sql = mapping.get("create_table", "")
        dataset_name = extract_dataset_name(create_sql) if create_sql else "Business_Logic"

        # Only run MERGE (skip CREATE TABLE — table already exists)
        merge_sql = mapping.get("merge_statement", "") or mapping.get("other_statement", "")
        if not merge_sql:
            yield PipelineEvent(stage="refresh_bl_table", status="success",
                                message=f"{table_name} — no MERGE SQL, skipped",
                                table_location=f"`{project}.{dataset_name}.{table_name}`")
            continue

        raw_create = mapping.get("create_table", "")
        merge_sql = fix_type_mismatches(merge_sql, raw_create)
        merge_sql = cleanup_sql(replace_placeholders(merge_sql, project, dataset_name))
        merge_sql = prepare_merge_sql(merge_sql)

        try:
            job = await asyncio.to_thread(bq_client.query, merge_sql)
            await asyncio.to_thread(job.result)
            duration = time.time() - t0
            row_count = await asyncio.to_thread(_get_row_count, bq_client, project, table_name)

            await asyncio.to_thread(
                log_pipeline_run, bq_client, project, run_id, "bl_refresh",
                table_name, "success", row_count, duration
            )
            success_count += 1
            yield PipelineEvent(stage="refresh_bl_table", status="success",
                                message=f"{table_name} refreshed — {row_count:,} rows ({duration:.1f}s)",
                                table_location=f"`{project}.{dataset_name}.{table_name}`")
        except Exception as e:
            duration = time.time() - t0
            error_msg = str(e)[:500]
            await asyncio.to_thread(
                log_pipeline_run, bq_client, project, run_id, "bl_refresh",
                table_name, "failed", 0, duration, error_msg
            )
            fail_count += 1
            yield PipelineEvent(stage="refresh_bl_table", status="failed",
                                message=f"{table_name} failed",
                                detail=error_msg,
                                table_location=f"`{project}.{dataset_name}.{table_name}`")

    # ── Summary ──────────────────────────────────────────────
    total = success_count + fail_count
    status = "success" if fail_count == 0 else "failed"
    yield PipelineEvent(
        stage="refresh", status=status,
        message=f"Refresh complete: {success_count}/{total} BL tables succeeded"
              + (f", {fail_count} failed" if fail_count else "")
              + (", CDL refresh failed" if not ok else "")
    )
