"""FastAPI web application for the Smart DTC Mapping Generator."""

import json
import os
import tempfile
from dataclasses import asdict
from pathlib import Path

import yaml
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .cli import GENERATORS, _classify_input, _parse_inputs
from .config import LAYERS, MAPPINGS_ROOT
from .pipeline import run_pipeline
from .sql_utils import cleanup_sql, ensure_datasets, extract_dataset_name, prepare_merge_sql, replace_placeholders
from .validator import MappingValidator

app = FastAPI(title="Smart DTC Mapping Generator")

STATIC_DIR = Path(__file__).parent / "static"


def _get_bq_client():
    """Get BigQuery client if available."""
    try:
        from google.cloud import bigquery
        project = os.environ.get("GCP_PROJECT_ID")
        return bigquery.Client(project=project) if project else bigquery.Client()
    except Exception:
        return None


def _check_existing_yaml(requirements, source_name: str) -> list[dict] | None:
    """Check if YAML already exists in pipeline_artifacts for the target tables.

    Returns a list of file dicts if found, None otherwise.
    """
    client = _get_bq_client()
    if not client:
        return None

    # Collect target table names from requirements
    target_tables = []
    for tm in requirements.table_mappings:
        tbl = tm.target_table or requirements.metadata.get("input_filename", "")
        if tbl:
            target_tables.append(tbl)
    if not target_tables:
        return None

    try:
        # Look for the latest version of each target table with status passed or executed
        placeholders = ", ".join(f"'{t}'" for t in target_tables)
        sql = f"""SELECT target_table, filename, yaml_content, version, status
                  FROM `{client.project}.Business_Logic.pipeline_artifacts`
                  WHERE target_table IN ({placeholders})
                    AND status IN ('passed', 'executed')
                  QUALIFY ROW_NUMBER() OVER (PARTITION BY target_table ORDER BY version DESC) = 1"""
        job = client.query(sql)
        rows = list(job.result())

        if not rows:
            return None

        # Build file list from stored artifacts
        output_files = []
        found_tables = set()
        for row in rows:
            found_tables.add(row.target_table)
            output_files.append({
                "filename": row.filename,
                "content": row.yaml_content,
                "warnings": [f"Loaded from storage (v{row.version}, status: {row.status})"],
                "validation_errors": [],
            })

        # Only return cached results if ALL target tables were found
        if set(target_tables) <= found_tables:
            return output_files
        return None
    except Exception:
        return None


@app.get("/", response_class=HTMLResponse)
async def index():
    return (STATIC_DIR / "index.html").read_text()


@app.get("/bq-status")
async def bq_status():
    """Check if BigQuery is available."""
    client = _get_bq_client()
    if client:
        return {"available": True, "project": client.project}
    return {"available": False, "project": None}


@app.post("/execute-bl")
async def execute_bl(
    yaml_content: str = Form(""),
    project_id: str = Form(""),
):
    """Execute BL SQL (CREATE TABLE + MERGE/INSERT) against BigQuery."""
    client = _get_bq_client()
    if not client:
        return JSONResponse(
            status_code=400,
            content={"errors": ["BigQuery not available. Set GCP_PROJECT_ID and ensure google-cloud-bigquery is installed."]},
        )

    if not yaml_content:
        return JSONResponse(
            status_code=400,
            content={"errors": ["No YAML content provided."]},
        )

    # Override project if provided
    if project_id:
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)

    try:
        mapping = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        return JSONResponse(
            status_code=400,
            content={"errors": [f"Invalid YAML: {e}"]},
        )

    results = []
    errors = []

    # Extract target dataset name
    create_sql_raw = mapping.get("create_table", "")
    dataset_name = extract_dataset_name(create_sql_raw) if create_sql_raw else "Business_Logic"

    # Ensure all required datasets exist
    created = ensure_datasets(client, client.project, dataset_name)
    for ds in created:
        results.append({"step": "create_dataset", "status": "success", "message": f"Created dataset {ds}"})

    project = client.project

    # Step 1: Execute CREATE TABLE
    create_sql = mapping.get("create_table", "")
    if create_sql:
        create_sql = cleanup_sql(replace_placeholders(create_sql, project, dataset_name))
        try:
            job = client.query(create_sql)
            job.result()
            results.append({"step": "create_table", "status": "success", "message": "Table created/verified"})
        except Exception as e:
            errors.append(f"CREATE TABLE failed: {e}")
            results.append({"step": "create_table", "status": "error", "message": str(e)})

    # Step 2: Execute MERGE or other_statement
    merge_sql = mapping.get("merge_statement", "") or mapping.get("other_statement", "")
    if merge_sql:
        merge_sql = cleanup_sql(replace_placeholders(merge_sql, project, dataset_name))
        merge_sql = prepare_merge_sql(merge_sql)

        try:
            job = client.query(merge_sql)
            job.result()
            affected = job.num_dml_affected_rows
            msg = "SQL executed successfully"
            if affected is not None:
                msg += f" ({affected:,} rows affected)"
            results.append({"step": "merge/insert", "status": "success", "message": msg})
        except Exception as e:
            errors.append(f"MERGE/INSERT failed: {e}")
            results.append({"step": "merge/insert", "status": "error", "message": str(e)})

    # Step 3: Get row count
    target_table = mapping.get("metadata", {}).get("target_table_name", "")
    if target_table and not errors:
        try:
            count_sql = f"SELECT COUNT(*) as cnt FROM `{project}.Business_Logic.{target_table}`"
            job = client.query(count_sql)
            row = list(job.result())[0]
            results.append({"step": "verify", "status": "success", "message": f"Table has {row.cnt:,} rows"})
        except Exception:
            pass  # Non-critical

    return {"results": results, "errors": errors}


@app.post("/execute-pipeline")
async def execute_pipeline(
    yaml_content: str = Form(""),
    filename: str = Form(""),
    project_id: str = Form(""),
):
    """Execute the full self-healing pipeline with SSE streaming."""
    client = _get_bq_client()
    if not client:
        return JSONResponse(
            status_code=400,
            content={"errors": ["BigQuery not available."]},
        )

    if not yaml_content:
        return JSONResponse(
            status_code=400,
            content={"errors": ["No YAML content provided."]},
        )

    if project_id:
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)

    async def event_stream():
        try:
            async for event in run_pipeline(yaml_content, filename, project_id or client.project, client):
                data = asdict(event)
                yield f"data: {json.dumps(data, default=str)}\n\n"
        except Exception as e:
            error_event = {
                "stage": "execute",
                "status": "failed",
                "message": f"Pipeline error: {str(e)}",
                "detail": str(e),
            }
            yield f"data: {json.dumps(error_event)}\n\n"
        yield "data: {\"stage\": \"done\", \"status\": \"done\", \"message\": \"Pipeline complete\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/pipeline-history")
async def pipeline_history(target_table: str = ""):
    """Retrieve past pipeline runs from BQ pipeline_artifacts table."""
    client = _get_bq_client()
    if not client:
        return {"runs": []}

    try:
        where = f"WHERE target_table = '{target_table}'" if target_table else ""
        sql = f"""SELECT artifact_id, filename, target_table, version, status,
                         error_message, attempt_number, created_at, updated_at
                  FROM `{client.project}.Business_Logic.pipeline_artifacts`
                  {where}
                  ORDER BY created_at DESC
                  LIMIT 50"""
        job = client.query(sql)
        rows = [dict(r.items()) for r in job.result()]
        return {"runs": rows}
    except Exception:
        return {"runs": []}


@app.post("/generate")
async def generate(
    csv_file: UploadFile | None = File(None),
    pdf_file: UploadFile | None = File(None),
    layer: str = Form(""),
    source: str = Form(""),
):
    if not csv_file and not pdf_file:
        return JSONResponse(
            status_code=400,
            content={"errors": ["Please upload at least one file (CSV/Excel or PDF)."]},
        )

    # Save uploaded files to temp directory
    tmp_dir = tempfile.mkdtemp(prefix="mapping_gen_")
    input_paths: list[str] = []

    try:
        for upload in [csv_file, pdf_file]:
            if upload and upload.filename:
                dest = Path(tmp_dir) / upload.filename
                dest.write_bytes(await upload.read())
                input_paths.append(str(dest))

        # Validate file types
        for p in input_paths:
            kind = _classify_input(Path(p))
            if kind is None:
                return JSONResponse(
                    status_code=400,
                    content={"errors": [f"Unsupported file format: {Path(p).suffix}"]},
                )

        # Parse inputs
        result = _parse_inputs(input_paths, api_key=None)
        if isinstance(result, int):
            return JSONResponse(
                status_code=400,
                content={"errors": ["Failed to parse input files. Check file format and content."]},
            )
        requirements = result

        # Resolve layer — default to BL if auto-detection fails
        resolved_layer = layer if layer else (requirements.layer or "BL")
        if resolved_layer not in LAYERS:
            resolved_layer = "BL"

        # Resolve source — derive from filename if not detected
        resolved_source = source if source else (requirements.source_name or "")
        if not resolved_source:
            resolved_source = Path(input_paths[0]).stem.replace("-", "_").upper()

        requirements.source_name = resolved_source
        requirements.layer = resolved_layer
        requirements.metadata["input_filename"] = requirements.metadata.get(
            "input_filename", Path(input_paths[0]).stem
        )

        # Check if YAML already exists in BQ for these target tables
        existing_files = _check_existing_yaml(requirements, resolved_source)
        if existing_files:
            return {"files": existing_files, "errors": [], "from_cache": True}

        # Generate via LLM
        generator_cls = GENERATORS[resolved_layer]
        generator = generator_cls(api_key=None, mappings_root=str(MAPPINGS_ROOT))
        files = generator.generate(requirements)

        if not files:
            return JSONResponse(
                status_code=500,
                content={"errors": ["No files were generated. The LLM may have returned invalid output."]},
            )

        # Validate and build response
        validator = MappingValidator()
        output_files = []
        for fn, content in files.items():
            errors = validator.validate(content, resolved_layer)
            warnings = [str(e) for e in errors if e.severity == "warning"]
            errs = [str(e) for e in errors if e.severity == "error"]
            output_files.append({
                "filename": fn,
                "content": content,
                "warnings": warnings,
                "validation_errors": errs,
            })

        return {"files": output_files, "errors": []}

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"errors": [f"Generation failed: {str(e)}"]},
        )
    finally:
        # Clean up temp files
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)
