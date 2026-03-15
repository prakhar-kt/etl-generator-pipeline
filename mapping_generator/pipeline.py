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
    fixed_yaml: str = ""
    test_name: str = ""
    test_results: list = field(default_factory=list)
    table_location: str = ""
    preview_rows: list = field(default_factory=list)


# ---------- BQ artifact storage ----------

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
    """Execute SQL from YAML with self-healing retry (max 3)."""
    project = bq_client.project or project_id
    current_yaml = yaml_content
    version = 1

    mapping = yaml.safe_load(current_yaml) or {}
    target_table = mapping.get("metadata", {}).get("target_table_name", "").split(".")[-1]
    dataset_name = "Business_Logic"
    create_sql = mapping.get("create_table", "")
    if create_sql:
        dataset_name = extract_dataset_name(create_sql)

    await asyncio.to_thread(ensure_datasets, bq_client, project, dataset_name)

    table_location = f"`{project}.{dataset_name}.{target_table}`"

    for attempt in range(1, MAX_RETRY + 1):
        yield PipelineEvent(stage="execute", status="running",
                            message=f"Executing SQL (attempt {attempt}/{MAX_RETRY})...",
                            attempt=attempt, version=version)

        ok, error_msg = await asyncio.to_thread(
            _execute_yaml_sql, bq_client, current_yaml, project, dataset_name
        )

        if ok:
            # Store the successful YAML version
            await asyncio.to_thread(
                upsert_latest_yaml, bq_client, project, target_table,
                filename, current_yaml, version, "executed"
            )
            yield PipelineEvent(stage="execute", status="success",
                                message="SQL executed successfully",
                                attempt=attempt, version=version,
                                table_location=table_location)
            return

        logger.warning(f"Execute attempt {attempt} failed: {error_msg}")

        if attempt < MAX_RETRY:
            yield PipelineEvent(stage="execute", status="retrying",
                                message=f"Execution failed, asking LLM to fix...",
                                attempt=attempt, version=version, detail=error_msg)
            try:
                fix = await asyncio.to_thread(call_llm_fix, current_yaml, error_msg, "execution")
                if fix.get("yaml"):
                    # Store the error→fix as a lesson for future learning
                    await asyncio.to_thread(
                        store_lesson, error_msg[:500],
                        f"LLM fixed execution error on attempt {attempt}",
                        "execution"
                    )
                    version += 1
                    current_yaml = fix["yaml"]
                    yield PipelineEvent(stage="execute", status="running",
                                        message=f"Retrying with LLM-fixed YAML (v{version})...",
                                        attempt=attempt + 1, version=version,
                                        fixed_yaml=current_yaml)
            except Exception as llm_err:
                yield PipelineEvent(stage="execute", status="failed",
                                    message="LLM fix failed", detail=str(llm_err))
                return
        else:
            # Store unfixed error as a lesson too
            await asyncio.to_thread(
                store_lesson, error_msg[:500],
                f"UNFIXED: execution failed after {MAX_RETRY} attempts",
                "execution_unfixed"
            )
            yield PipelineEvent(stage="execute", status="failed",
                                message=f"Failed after {MAX_RETRY} attempts",
                                attempt=attempt, version=version, detail=error_msg)
            return


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


# ---------- Pipeline: Tests (sequential, per-test retry) ----------

async def run_tests(
    yaml_content: str,
    project_id: str,
    bq_client,
    filename: str = "",
) -> AsyncGenerator[PipelineEvent, None]:
    """Run DQ tests one by one. Each test retries up to 3 times via LLM fix."""
    project = bq_client.project or project_id
    current_yaml = yaml_content
    version = 1

    try:
        tests = generate_tests(current_yaml, project)
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

    mapping = yaml.safe_load(re.sub(r'\{\{[^}]*\}\}', 'X', current_yaml)) or {}
    dataset_name = extract_dataset_name(mapping.get("create_table", ""))
    target_table = mapping.get("metadata", {}).get("target_table_name", "").split(".")[-1]

    all_results = []

    for test in tests:
        test_passed = False
        current_test = dict(test)  # allow test SQL to be modified

        for attempt in range(1, MAX_RETRY + 1):
            yield PipelineEvent(stage="test_item", status="running",
                                message=f"Running {test['name']} (attempt {attempt}/{MAX_RETRY})...",
                                test_name=test["name"], attempt=attempt)

            # Run the test
            try:
                if current_test.get("_dynamic"):
                    result = {"name": test["name"], "status": "skip", "detail": "Skipped"}
                    test_passed = True
                else:
                    job = await asyncio.to_thread(bq_client.query, current_test["sql"])
                    rows_raw = await asyncio.to_thread(lambda: list(job.result()))
                    rows = [dict(r.items()) for r in rows_raw]
                    result = evaluate_test_result(current_test, rows)
            except Exception as e:
                result = {"name": test["name"], "status": "fail",
                          "detail": f"Test query error: {str(e)[:300]}",
                          "severity": test.get("severity", "error")}

            if result["status"] == "pass" or result["status"] == "skip":
                test_passed = True
                yield PipelineEvent(stage="test_item", status="success",
                                    message=f"{test['name']} passed",
                                    test_name=test["name"], attempt=attempt,
                                    detail=result.get("detail", ""))
                all_results.append(result)
                break

            # Test failed
            if result.get("severity") == "warning":
                # Warnings don't block — mark as pass with warning
                yield PipelineEvent(stage="test_item", status="success",
                                    message=f"{test['name']} warning",
                                    test_name=test["name"], attempt=attempt,
                                    detail=result.get("detail", ""))
                all_results.append(result)
                test_passed = True
                break

            if attempt < MAX_RETRY:
                yield PipelineEvent(stage="test_item", status="retrying",
                                    message=f"{test['name']} failed, asking LLM to fix...",
                                    test_name=test["name"], attempt=attempt,
                                    detail=result.get("detail", ""))
                try:
                    error_ctx = f"Test '{test['name']}' ({test['description']}) failed: {result['detail']}"
                    fix = await asyncio.to_thread(
                        call_llm_fix, current_yaml, error_ctx, "test_fix",
                        current_test["sql"]
                    )
                    if fix.get("yaml"):
                        # Store lesson and persist fixed YAML
                        await asyncio.to_thread(
                            store_lesson, error_ctx[:500],
                            f"Fixed {test['name']} by modifying YAML SQL on attempt {attempt}",
                            f"test_{test['name']}"
                        )
                        version += 1
                        current_yaml = fix["yaml"]
                        await asyncio.to_thread(
                            upsert_latest_yaml, bq_client, project, target_table,
                            filename, current_yaml, version, "testing"
                        )
                        # Re-execute YAML then re-run test
                        ok, err = await asyncio.to_thread(
                            _execute_yaml_sql, bq_client, current_yaml, project, dataset_name
                        )
                        if not ok:
                            yield PipelineEvent(stage="test_item", status="retrying",
                                                message=f"Re-execution failed: {err[:200]}",
                                                test_name=test["name"], attempt=attempt)
                        # Regenerate tests with new YAML
                        new_tests = generate_tests(current_yaml, project)
                        # Find the matching test by name
                        for nt in new_tests:
                            if nt["name"] == test["name"]:
                                current_test = nt
                                break
                        yield PipelineEvent(stage="test_item", status="running",
                                            message=f"Retrying {test['name']} with fixed YAML...",
                                            test_name=test["name"], attempt=attempt + 1,
                                            fixed_yaml=current_yaml)
                    elif fix.get("test_sql"):
                        # Store lesson: test failure fixed by test SQL change
                        await asyncio.to_thread(
                            store_lesson, error_ctx[:500],
                            f"Fixed {test['name']} by modifying test SQL on attempt {attempt}",
                            f"test_{test['name']}"
                        )
                        current_test["sql"] = fix["test_sql"]
                        yield PipelineEvent(stage="test_item", status="running",
                                            message=f"Retrying {test['name']} with fixed test SQL...",
                                            test_name=test["name"], attempt=attempt + 1)
                except Exception as llm_err:
                    logger.error(f"LLM fix failed for {test['name']}: {llm_err}")
            else:
                # Max retries reached — stop pipeline
                all_results.append(result)
                yield PipelineEvent(stage="test_item", status="failed",
                                    message=f"{test['name']} failed after {MAX_RETRY} attempts",
                                    test_name=test["name"], attempt=attempt,
                                    detail=result.get("detail", ""))
                yield PipelineEvent(stage="test", status="failed",
                                    message=f"Pipeline stopped: {test['name']} failed",
                                    detail=result.get("detail", ""),
                                    test_results=all_results)
                return

        if not test_passed:
            return

    # All tests passed — persist final YAML as "passed"
    await asyncio.to_thread(
        upsert_latest_yaml, bq_client, project, target_table,
        filename, current_yaml, version, "passed"
    )
    yield PipelineEvent(stage="test", status="success",
                        message=f"All {len(all_results)} tests passed",
                        test_results=all_results)


# Need re import for the regex in run_tests
import re
