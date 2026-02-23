"""FastAPI web application for the Smart DTC Mapping Generator."""

import tempfile
from pathlib import Path

from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .cli import GENERATORS, _classify_input, _parse_inputs
from .config import LAYERS, MAPPINGS_ROOT
from .validator import MappingValidator

app = FastAPI(title="Smart DTC Mapping Generator")

STATIC_DIR = Path(__file__).parent / "static"


@app.get("/", response_class=HTMLResponse)
async def index():
    return (STATIC_DIR / "index.html").read_text()


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

        # Resolve layer
        resolved_layer = layer if layer else (requirements.layer or "")
        if not resolved_layer:
            return JSONResponse(
                status_code=400,
                content={"errors": ["Could not auto-detect layer. Please select a layer."]},
            )
        if resolved_layer not in LAYERS:
            return JSONResponse(
                status_code=400,
                content={"errors": [f"Invalid layer: {resolved_layer}"]},
            )

        # Resolve source
        resolved_source = source if source else (requirements.source_name or "")
        if not resolved_source:
            return JSONResponse(
                status_code=400,
                content={"errors": ["Could not auto-detect source. Please enter a source name."]},
            )

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
