"""FastAPI web application for the Smart DTC Mapping Generator."""

import json
import os
import tempfile
import uuid
from dataclasses import asdict
from pathlib import Path

import yaml
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from .cli import GENERATORS, _classify_input, _parse_inputs
from .config import LAYERS, MAPPINGS_ROOT
from .pipeline import (
    PipelineEvent, ensure_artifacts_table, ensure_datasets, extract_dataset_name,
    run_execute, run_preview, run_tests, store_artifact,
)
from .sql_utils import cleanup_sql, prepare_merge_sql, replace_placeholders
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
    """Check if YAML already exists in pipeline_artifacts for the target tables."""
    client = _get_bq_client()
    if not client:
        return None

    target_tables = []
    for tm in requirements.table_mappings:
        tbl = tm.target_table or requirements.metadata.get("input_filename", "")
        if tbl:
            target_tables.append(tbl)
    if not target_tables:
        return None

    try:
        placeholders = ", ".join(f"'{t}'" for t in target_tables)
        # Pick latest version, preferring passed > executed > testing > generated
        sql = f"""SELECT target_table, filename, yaml_content, version, status
                  FROM `{client.project}.Business_Logic.pipeline_artifacts`
                  WHERE target_table IN ({placeholders})
                  QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY target_table
                    ORDER BY
                      CASE status WHEN 'passed' THEN 1 WHEN 'executed' THEN 2 WHEN 'testing' THEN 3 WHEN 'generated' THEN 4 ELSE 5 END,
                      version DESC
                  ) = 1"""
        job = client.query(sql)
        rows = list(job.result())
        if not rows:
            return None

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

        if set(target_tables) <= found_tables:
            return output_files
        return None
    except Exception:
        return None


def _store_yaml_in_bq(filename: str, target_table: str, yaml_content: str):
    """Store generated YAML in pipeline_artifacts."""
    client = _get_bq_client()
    if not client:
        return
    try:
        project = client.project
        ensure_datasets(client, project)
        ensure_artifacts_table(client, project)
        artifact_id = str(uuid.uuid4())
        store_artifact(client, project, artifact_id, filename, target_table,
                       yaml_content, 1, "generated")
    except Exception:
        pass  # non-critical


@app.get("/", response_class=HTMLResponse)
async def index():
    return (STATIC_DIR / "index.html").read_text()


@app.get("/bq-status")
async def bq_status():
    client = _get_bq_client()
    if client:
        return {"available": True, "project": client.project}
    return {"available": False, "project": None}


@app.post("/execute-pipeline")
async def execute_pipeline(
    yaml_content: str = Form(""),
    filename: str = Form(""),
    project_id: str = Form(""),
):
    """Execute SQL from YAML with self-healing retry. SSE stream."""
    client = _get_bq_client()
    if not client:
        return JSONResponse(status_code=400, content={"errors": ["BigQuery not available."]})
    if not yaml_content:
        return JSONResponse(status_code=400, content={"errors": ["No YAML content provided."]})
    if project_id:
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)

    async def event_stream():
        try:
            async for event in run_execute(yaml_content, filename, project_id or client.project, client):
                yield f"data: {json.dumps(asdict(event), default=str)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'stage': 'execute', 'status': 'failed', 'message': str(e)})}\n\n"
        yield "data: {\"stage\": \"done\", \"status\": \"done\", \"message\": \"Complete\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/preview-table")
async def preview_table(
    target_table: str = Form(""),
    project_id: str = Form(""),
):
    """Return top 10 rows from a BQ table."""
    client = _get_bq_client()
    if not client:
        return JSONResponse(status_code=400, content={"errors": ["BigQuery not available."]})
    if project_id:
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)
    try:
        rows = await run_preview(target_table, project_id or client.project, client)
        # Convert non-serializable types
        clean_rows = []
        for row in rows:
            clean = {}
            for k, v in row.items():
                clean[k] = str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
            clean_rows.append(clean)
        return {"rows": clean_rows, "count": len(clean_rows)}
    except Exception as e:
        return JSONResponse(status_code=400, content={"errors": [str(e)]})


@app.post("/run-tests")
async def run_tests_endpoint(
    yaml_content: str = Form(""),
    filename: str = Form(""),
    project_id: str = Form(""),
):
    """Run DQ tests one by one with per-test self-healing. SSE stream."""
    client = _get_bq_client()
    if not client:
        return JSONResponse(status_code=400, content={"errors": ["BigQuery not available."]})
    if not yaml_content:
        return JSONResponse(status_code=400, content={"errors": ["No YAML content provided."]})
    if project_id:
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)

    async def event_stream():
        try:
            async for event in run_tests(yaml_content, project_id or client.project, client, filename):
                yield f"data: {json.dumps(asdict(event), default=str)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'stage': 'test', 'status': 'failed', 'message': str(e)})}\n\n"
        yield "data: {\"stage\": \"done\", \"status\": \"done\", \"message\": \"Complete\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/pipeline-history")
async def pipeline_history(target_table: str = ""):
    client = _get_bq_client()
    if not client:
        return {"runs": []}
    try:
        where = f"WHERE target_table = '{target_table}'" if target_table else ""
        sql = f"""SELECT artifact_id, filename, target_table, version, status,
                         error_message, attempt_number, created_at, updated_at
                  FROM `{client.project}.Business_Logic.pipeline_artifacts`
                  {where}
                  ORDER BY created_at DESC LIMIT 50"""
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
        return JSONResponse(status_code=400,
                            content={"errors": ["Please upload at least one file (CSV/Excel or PDF)."]})

    tmp_dir = tempfile.mkdtemp(prefix="mapping_gen_")
    input_paths: list[str] = []

    try:
        for upload in [csv_file, pdf_file]:
            if upload and upload.filename:
                dest = Path(tmp_dir) / upload.filename
                dest.write_bytes(await upload.read())
                input_paths.append(str(dest))

        for p in input_paths:
            kind = _classify_input(Path(p))
            if kind is None:
                return JSONResponse(status_code=400,
                                    content={"errors": [f"Unsupported file format: {Path(p).suffix}"]})

        result = _parse_inputs(input_paths, api_key=None)
        if isinstance(result, int):
            return JSONResponse(status_code=400,
                                content={"errors": ["Failed to parse input files."]})
        requirements = result

        resolved_layer = layer if layer else (requirements.layer or "BL")
        if resolved_layer not in LAYERS:
            resolved_layer = "BL"

        resolved_source = source if source else (requirements.source_name or "")
        if not resolved_source:
            resolved_source = Path(input_paths[0]).stem.replace("-", "_").upper()

        requirements.source_name = resolved_source
        requirements.layer = resolved_layer
        requirements.metadata["input_filename"] = requirements.metadata.get(
            "input_filename", Path(input_paths[0]).stem
        )

        # Check cache first
        existing = _check_existing_yaml(requirements, resolved_source)
        if existing:
            return {"files": existing, "errors": [], "from_cache": True}

        # Generate via LLM
        generator_cls = GENERATORS[resolved_layer]
        generator = generator_cls(api_key=None, mappings_root=str(MAPPINGS_ROOT))
        files = generator.generate(requirements)

        if not files:
            return JSONResponse(status_code=500,
                                content={"errors": ["No files were generated."]})

        # Validate, store, and build response
        validator = MappingValidator()
        output_files = []
        for fn, content in files.items():
            errors = validator.validate(content, resolved_layer)
            warnings = [str(e) for e in errors if e.severity == "warning"]
            errs = [str(e) for e in errors if e.severity == "error"]

            # Extract target table for storage
            try:
                safe = yaml.safe_load(content.replace("{{", "X").replace("}}", "X")) or {}
                tgt = safe.get("metadata", {}).get("target_table_name", "").split(".")[-1]
            except Exception:
                tgt = fn.replace(".yml", "")

            # Store in BQ
            _store_yaml_in_bq(fn, tgt, content)

            output_files.append({
                "filename": fn,
                "content": content,
                "warnings": warnings,
                "validation_errors": errs,
            })

        return {"files": output_files, "errors": []}

    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"errors": [f"Generation failed: {str(e)}"]})
    finally:
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)
