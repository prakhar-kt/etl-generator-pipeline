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
from .sql_utils import cleanup_sql, ensure_datasets, extract_dataset_name, prepare_merge_sql, replace_placeholders
from .test_generator import evaluate_test_result, generate_tests

logger = logging.getLogger("pipeline")

MAX_ATTEMPTS = 5


@dataclass
class PipelineEvent:
    stage: str  # "store", "execute", "test"
    status: str  # "pending", "running", "success", "failed", "retrying"
    message: str
    attempt: int = 0
    max_attempts: int = MAX_ATTEMPTS
    version: int = 1
    detail: str = ""
    fixed_yaml: str = ""  # non-empty when LLM fixes YAML
    test_results: list = field(default_factory=list)


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


def _esc(s: str) -> str:
    """Escape single quotes for BQ SQL strings."""
    return s.replace("'", "\\'").replace("\\", "\\\\") if s else ""


# ---------- LLM fix ----------

def call_llm_fix(yaml_content: str, error_message: str, context: str = "execution") -> str:
    """Send YAML + error to LLM, get back fixed YAML."""
    client = _create_llm_client(LLM_PROVIDER)

    if context == "execution":
        system_prompt = """You are a BigQuery SQL expert. The user will provide a YAML mapping file
that contains BigQuery SQL (CREATE TABLE and MERGE statements). The SQL failed with an error.
Fix the YAML so the SQL executes successfully. Common issues:
- Wrong column names (CDL tables use CDL_LOAD_DATE, not LOAD_DATE)
- Missing GROUP BY columns
- Invalid STRUCT references (use explicit column lists instead)
- AS aliases in VALUES clauses (BigQuery doesn't allow them)
- References to non-existent columns in source tables

Return ONLY the corrected YAML content. No markdown fences. No explanation."""
    else:
        system_prompt = """You are a BigQuery SQL expert. The user will provide a YAML mapping file
and the results of data quality tests that failed. Fix the YAML's SQL so the tests pass.
Common fixes:
- Add COALESCE for NULL values (use 0 for numeric, 'N/A' for string)
- Fix GROUP BY to include all non-aggregated columns
- Fix JOIN conditions to prevent duplicate rows
- Ensure ADMIN_COMPOSITEKEY_HASH is deterministic and unique

Return ONLY the corrected YAML content. No markdown fences. No explanation."""

    user_prompt = f"""## Error
{error_message}

## Current YAML (fix this)
{yaml_content}"""

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

    # Strip markdown fences if present
    lines = result.split("\n")
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()


# ---------- SQL execution helpers ----------

def _execute_sql(client, sql: str) -> tuple[bool, str, int | None]:
    """Execute a SQL statement. Returns (success, message, affected_rows)."""
    try:
        job = client.query(sql)
        job.result()
        affected = getattr(job, 'num_dml_affected_rows', None)
        msg = "SQL executed successfully"
        if affected is not None:
            msg += f" ({affected:,} rows affected)"
        return True, msg, affected
    except Exception as e:
        return False, str(e), None


def _execute_yaml_sql(client, yaml_content: str, project: str, dataset_name: str) -> tuple[bool, str]:
    """Execute CREATE TABLE + MERGE from YAML. Returns (success, error_message)."""
    mapping = yaml.safe_load(yaml_content)
    errors = []

    # CREATE TABLE
    create_sql = mapping.get("create_table", "")
    if create_sql:
        create_sql = cleanup_sql(replace_placeholders(create_sql, project, dataset_name))
        ok, msg, _ = _execute_sql(client, create_sql)
        if not ok:
            return False, f"CREATE TABLE failed: {msg}"

    # MERGE / INSERT
    merge_sql = mapping.get("merge_statement", "") or mapping.get("other_statement", "")
    if merge_sql:
        merge_sql = cleanup_sql(replace_placeholders(merge_sql, project, dataset_name))
        merge_sql = prepare_merge_sql(merge_sql)
        ok, msg, _ = _execute_sql(client, merge_sql)
        if not ok:
            return False, f"MERGE/INSERT failed: {msg}"

    return True, ""


# ---------- Pipeline orchestration ----------

async def run_pipeline(
    yaml_content: str,
    filename: str,
    project_id: str,
    bq_client,
) -> AsyncGenerator[PipelineEvent, None]:
    """Run the full self-healing pipeline, yielding events for each stage."""
    project = bq_client.project or project_id
    artifact_id = str(uuid.uuid4())
    current_yaml = yaml_content
    version = 1

    # Extract target table and dataset
    mapping = yaml.safe_load(yaml_content)
    target_table = ""
    dataset_name = "Business_Logic"
    if mapping:
        target_table = mapping.get("metadata", {}).get("target_table_name", "").split(".")[-1]
        create_sql = mapping.get("create_table", "")
        if create_sql:
            dataset_name = extract_dataset_name(create_sql)

    # --- STORE STAGE ---
    yield PipelineEvent(stage="store", status="running", message="Storing YAML artifact...")

    try:
        await asyncio.to_thread(ensure_datasets, bq_client, project, dataset_name)
        await asyncio.to_thread(ensure_artifacts_table, bq_client, project)
        await asyncio.to_thread(
            store_artifact, bq_client, project, artifact_id, filename,
            target_table, current_yaml, version, "generated"
        )
        yield PipelineEvent(stage="store", status="success", message="YAML stored in BigQuery",
                            version=version)
    except Exception as e:
        logger.error(f"Store failed: {e}")
        yield PipelineEvent(stage="store", status="failed", message="Failed to store artifact",
                            detail=str(e))
        return

    # --- EXECUTE STAGE (with retry) ---
    yield PipelineEvent(stage="execute", status="running", message="Executing SQL...",
                        attempt=1, version=version)

    execute_success = False
    for attempt in range(1, MAX_ATTEMPTS + 1):
        ok, error_msg = await asyncio.to_thread(
            _execute_yaml_sql, bq_client, current_yaml, project, dataset_name
        )

        if ok:
            execute_success = True
            await asyncio.to_thread(
                update_artifact, bq_client, project, artifact_id, version,
                status="executed"
            )
            yield PipelineEvent(stage="execute", status="success",
                                message="SQL executed successfully",
                                attempt=attempt, version=version)
            break
        else:
            logger.warning(f"Execute attempt {attempt} failed: {error_msg}")
            if attempt < MAX_ATTEMPTS:
                yield PipelineEvent(stage="execute", status="retrying",
                                    message=f"Execution failed, asking LLM to fix...",
                                    attempt=attempt, version=version, detail=error_msg)
                # Ask LLM to fix
                try:
                    fixed_yaml = await asyncio.to_thread(
                        call_llm_fix, current_yaml, error_msg, "execution"
                    )
                    version += 1
                    current_yaml = fixed_yaml
                    # Store new version
                    await asyncio.to_thread(
                        store_artifact, bq_client, project, artifact_id, filename,
                        target_table, current_yaml, version, "executing"
                    )
                    yield PipelineEvent(stage="execute", status="running",
                                        message=f"Retrying with LLM-fixed YAML (v{version})...",
                                        attempt=attempt + 1, version=version,
                                        fixed_yaml=current_yaml)
                except Exception as llm_err:
                    yield PipelineEvent(stage="execute", status="failed",
                                        message=f"LLM fix failed",
                                        attempt=attempt, detail=str(llm_err))
                    return
            else:
                await asyncio.to_thread(
                    update_artifact, bq_client, project, artifact_id, version,
                    status="failed", error_message=error_msg
                )
                yield PipelineEvent(stage="execute", status="failed",
                                    message=f"Failed after {MAX_ATTEMPTS} attempts",
                                    attempt=attempt, version=version, detail=error_msg)
                return

    if not execute_success:
        return

    # --- TEST STAGE (with retry) ---
    yield PipelineEvent(stage="test", status="running", message="Generating and running DQ tests...",
                        attempt=1, version=version)

    try:
        tests = generate_tests(current_yaml, project)
    except Exception as e:
        yield PipelineEvent(stage="test", status="failed", message="Failed to generate tests",
                            detail=str(e))
        return

    # Store test definitions
    try:
        await asyncio.to_thread(
            update_artifact, bq_client, project, artifact_id, version,
            status="testing", test_definitions=json.dumps(tests, default=str)
        )
    except Exception:
        pass  # non-critical

    for test_attempt in range(1, MAX_ATTEMPTS + 1):
        # Run all tests
        all_results = []
        failures = []

        for test in tests:
            if test.get("_dynamic"):
                all_results.append({"name": test["name"], "status": "skip", "detail": "Dynamic test skipped"})
                continue
            try:
                job = await asyncio.to_thread(bq_client.query, test["sql"])
                rows_raw = await asyncio.to_thread(lambda: list(job.result()))
                rows = [dict(r.items()) for r in rows_raw]
                result = evaluate_test_result(test, rows)
                all_results.append(result)
                if result["status"] == "fail" and result["severity"] == "error":
                    failures.append(result)
            except Exception as e:
                all_results.append({"name": test["name"], "status": "error",
                                    "detail": f"Test query failed: {str(e)[:200]}"})

        if not failures:
            # All tests passed
            try:
                await asyncio.to_thread(
                    update_artifact, bq_client, project, artifact_id, version,
                    status="passed", test_results=json.dumps(all_results)
                )
            except Exception:
                pass
            yield PipelineEvent(stage="test", status="success",
                                message=f"All DQ tests passed ({len(all_results)} tests)",
                                attempt=test_attempt, version=version,
                                test_results=all_results)
            return
        else:
            # Tests failed
            failure_detail = "; ".join(f"{f['name']}: {f['detail']}" for f in failures)
            if test_attempt < MAX_ATTEMPTS:
                yield PipelineEvent(stage="test", status="retrying",
                                    message=f"{len(failures)} test(s) failed, asking LLM to fix...",
                                    attempt=test_attempt, version=version,
                                    detail=failure_detail, test_results=all_results)
                # Ask LLM to fix based on test failures
                try:
                    fix_context = f"Test failures:\n{failure_detail}\n\nTest definitions:\n"
                    fix_context += "\n".join(f"- {t['name']}: {t['description']}" for t in tests)
                    fixed_yaml = await asyncio.to_thread(
                        call_llm_fix, current_yaml, fix_context, "test"
                    )
                    version += 1
                    current_yaml = fixed_yaml
                    await asyncio.to_thread(
                        store_artifact, bq_client, project, artifact_id, filename,
                        target_table, current_yaml, version, "testing"
                    )
                    # Re-execute with fixed YAML
                    ok, err = await asyncio.to_thread(
                        _execute_yaml_sql, bq_client, current_yaml, project, dataset_name
                    )
                    if not ok:
                        yield PipelineEvent(stage="test", status="failed",
                                            message="Re-execution failed after test fix",
                                            attempt=test_attempt, detail=err)
                        return
                    # Regenerate tests for new YAML
                    tests = generate_tests(current_yaml, project)
                    yield PipelineEvent(stage="test", status="running",
                                        message=f"Re-testing with LLM-fixed YAML (v{version})...",
                                        attempt=test_attempt + 1, version=version,
                                        fixed_yaml=current_yaml)
                except Exception as llm_err:
                    yield PipelineEvent(stage="test", status="failed",
                                        message="LLM fix for tests failed",
                                        attempt=test_attempt, detail=str(llm_err))
                    return
            else:
                try:
                    await asyncio.to_thread(
                        update_artifact, bq_client, project, artifact_id, version,
                        status="failed", test_results=json.dumps(all_results),
                        error_message=failure_detail
                    )
                except Exception:
                    pass
                yield PipelineEvent(stage="test", status="failed",
                                    message=f"Tests failed after {MAX_ATTEMPTS} attempts",
                                    attempt=test_attempt, version=version,
                                    detail=failure_detail, test_results=all_results)
                return
