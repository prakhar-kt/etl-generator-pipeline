"""FastAPI web application for the Smart DTC Mapping Generator."""

import os
import tempfile
from pathlib import Path

import yaml
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .cli import GENERATORS, _classify_input, _parse_inputs
from .config import LAYERS, MAPPINGS_ROOT
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

    # Ensure the target dataset exists (extract from metadata or SQL)
    target_table = mapping.get("metadata", {}).get("target_table_name", "")
    # Extract dataset name from CREATE TABLE SQL: `project.dataset.table`
    create_sql_raw = mapping.get("create_table", "")
    dataset_name = None
    if create_sql_raw:
        import re
        m = re.search(r'`[^`]+\.([^`]+)\.[^`]+`', create_sql_raw)
        if m:
            dataset_name = m.group(1)
    if not dataset_name:
        dataset_name = "Business_Logic"

    try:
        from google.cloud import bigquery as bq
        dataset_ref = bq.DatasetReference(client.project, dataset_name)
        client.get_dataset(dataset_ref)
    except Exception:
        try:
            dataset = bq.Dataset(dataset_ref)
            dataset.location = "US"
            client.create_dataset(dataset)
            results.append({"step": "create_dataset", "status": "success", "message": f"Created dataset {dataset_name}"})
        except Exception as e:
            errors.append(f"Failed to create dataset {dataset_name}: {e}")
            results.append({"step": "create_dataset", "status": "error", "message": str(e)})

    # Replace Jinja2 placeholders with actual project
    def replace_placeholders(sql: str) -> str:
        project = client.project
        sql = sql.replace("{{ target_project }}", project)
        sql = sql.replace("{{ source_projects[0] }}", project)
        sql = sql.replace("{{ source_projects[1] }}", project)
        sql = sql.replace("{{ source_projects[2] }}", project)
        sql = sql.replace("{{ process_id }}", "web-ui-exec")
        return sql

    # Step 1: Execute CREATE TABLE
    create_sql = mapping.get("create_table", "")
    if create_sql:
        create_sql = replace_placeholders(create_sql)
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
        merge_sql = replace_placeholders(merge_sql)

        # Replace get_max_date inline if referenced
        max_date_sql = mapping.get("get_max_date", "")
        if max_date_sql:
            max_date_sql = replace_placeholders(max_date_sql).rstrip(";").strip()

        try:
            job = client.query(merge_sql)
            job.result()
            affected = job.num_dml_affected_rows
            msg = f"SQL executed successfully"
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
            count_sql = f"SELECT COUNT(*) as cnt FROM `{client.project}.Business_Logic.{target_table}`"
            job = client.query(count_sql)
            row = list(job.result())[0]
            results.append({"step": "verify", "status": "success", "message": f"Table has {row.cnt:,} rows"})
        except Exception:
            pass  # Non-critical

    return {"results": results, "errors": errors}


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

        # Generate
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
        for filename, content in files.items():
            errors = validator.validate(content, resolved_layer)
            warnings = [str(e) for e in errors if e.severity == "warning"]
            errs = [str(e) for e in errors if e.severity == "error"]
            output_files.append({
                "filename": filename,
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
