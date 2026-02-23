"""CLI entry point for the Smart DTC Mapping Generator pipeline."""

import argparse
import json
import sys
from pathlib import Path

from .config import LAYERS, MAPPINGS_ROOT
from .generators import BLGenerator, BRGenerator, CDLGenerator, CleansedRawGenerator
from .parsers import CSVParser, PDFParser
from .parsers.merge import merge_requirements
from .source_checker import SourceTableChecker
from .validator import MappingValidator


GENERATORS = {
    "CDL": CDLGenerator,
    "Cleansed_RAW": CleansedRawGenerator,
    "BL": BLGenerator,
    "BR": BRGenerator,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mapping_generator",
        description="Generate Smart DTC YAML mapping files from PDF/CSV requirements using Claude API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate CDL mapping from CSV
  python -m mapping_generator --input requirements.csv --layer CDL --source PRP1

  # Generate BL mapping from PDF
  python -m mapping_generator --input spec.pdf --layer BL --source SHOPIFY_MC

  # Auto-detect layer from CSV content
  python -m mapping_generator --input requirements.csv --source PRP1

  # Specify custom output directory
  python -m mapping_generator --input reqs.csv --layer BL --source PRP1 --output ./output/

  # Generate BL mapping from both CSV (fields) + PDF (business rules)
  python -m mapping_generator --input reqs.csv spec.pdf --layer BL --source Various

  # Validate an existing mapping file
  python -m mapping_generator --validate path/to/mapping.yml --layer BL
        """,
    )

    parser.add_argument(
        "--input", "-i",
        nargs="+",
        type=str,
        help="Path to 1 or 2 requirements files. When 2 are provided, one must be CSV/Excel and the other PDF. CSV provides field definitions; PDF provides business rules.",
    )
    parser.add_argument(
        "--layer", "-l",
        type=str,
        choices=list(LAYERS.keys()),
        help="Target layer (CDL, Cleansed_RAW, BL, BR). Auto-detected from input if not specified.",
    )
    parser.add_argument(
        "--source", "-s",
        type=str,
        help="Data source name (e.g. PRP1, SHOPIFY_MC, ANAPLAN). Auto-detected from input if not specified.",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        help="Output directory. Defaults to Mappings/{layer}/{source}/",
    )
    parser.add_argument(
        "--mappings-root",
        type=str,
        default=str(MAPPINGS_ROOT),
        help="Path to existing Smart_DTC Mappings folder (for few-shot examples)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        help="Anthropic API key (or set ANTHROPIC_API_KEY env var)",
    )
    parser.add_argument(
        "--validate",
        type=str,
        help="Validate an existing YAML mapping file instead of generating",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print generated YAML to stdout instead of writing files",
    )
    parser.add_argument(
        "--skip-source-check",
        action="store_true",
        help="Skip validation that source tables exist as CDL/BL tables in the Mappings folder",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Launch the web UI instead of running the CLI pipeline",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for the web server (default: 8000, used with --serve)",
    )

    args = parser.parse_args(argv)

    if not args.input and not args.validate and not args.serve:
        parser.error("Either --input, --validate, or --serve is required")

    return args


def run_validation(args: argparse.Namespace) -> int:
    """Validate an existing mapping file."""
    if not args.layer:
        print("Error: --layer is required for validation", file=sys.stderr)
        return 1

    validator = MappingValidator()
    errors = validator.validate_file(args.validate, args.layer)

    if not errors:
        print(f"VALID: {args.validate} passes all {args.layer} layer checks")
        return 0

    error_count = sum(1 for e in errors if e.severity == "error")
    warning_count = sum(1 for e in errors if e.severity == "warning")

    for err in errors:
        print(err)

    print(f"\n{error_count} error(s), {warning_count} warning(s)")
    return 1 if error_count > 0 else 0


def _classify_input(path: Path) -> str | None:
    """Return 'csv' or 'pdf' based on file extension, or None if unsupported."""
    ext = path.suffix.lower()
    if ext in (".csv", ".xlsx", ".xls"):
        return "csv"
    if ext == ".pdf":
        return "pdf"
    return None


def _parse_inputs(input_paths: list[str], api_key: str | None) -> "ParsedRequirements | int":
    """Parse 1 or 2 input files and merge if both are provided.

    Returns ParsedRequirements on success, or an int error code on failure.
    """
    from .parsers.csv_parser import ParsedRequirements  # noqa: F811

    paths = [Path(p) for p in input_paths]

    # Validate file count
    if len(paths) > 2:
        print("Error: At most 2 input files are allowed (one CSV/Excel, one PDF)", file=sys.stderr)
        return 1

    # Validate files exist and have supported formats
    classified: dict[str, Path] = {}
    for p in paths:
        if not p.exists():
            print(f"Error: Input file not found: {p}", file=sys.stderr)
            return 1
        kind = _classify_input(p)
        if kind is None:
            print(f"Error: Unsupported file format: {p.suffix}", file=sys.stderr)
            return 1
        if kind in classified:
            print(f"Error: Two {kind.upper()} files provided. When using 2 inputs, one must be CSV/Excel and the other PDF.", file=sys.stderr)
            return 1
        classified[kind] = p

    # Parse each file
    parsed: dict[str, ParsedRequirements] = {}
    for kind, p in classified.items():
        print(f"Parsing {p.name}...")
        if kind == "pdf":
            parser = PDFParser(api_key=api_key)
        else:
            parser = CSVParser()
        parsed[kind] = parser.parse(p)

    # Merge or return single result
    if "csv" in parsed and "pdf" in parsed:
        print("Merging CSV field definitions with PDF business rules...")
        return merge_requirements(parsed["csv"], parsed["pdf"])
    elif "csv" in parsed:
        return parsed["csv"]
    else:
        return parsed["pdf"]


def run_generation(args: argparse.Namespace) -> int:
    """Parse requirements and generate mapping files."""
    result = _parse_inputs(args.input, args.api_key)
    if isinstance(result, int):
        return result
    requirements = result

    # Resolve layer
    layer = args.layer or requirements.layer
    if not layer:
        print("Error: Could not auto-detect layer. Please specify --layer", file=sys.stderr)
        return 1
    print(f"Target layer: {layer}")

    # Resolve source name
    source = args.source or requirements.source_name
    if not source:
        print("Error: Could not auto-detect source. Please specify --source", file=sys.stderr)
        return 1
    requirements.source_name = source
    requirements.layer = layer
    requirements.metadata["input_filename"] = requirements.metadata.get(
        "input_filename", Path(args.input[0]).stem
    )
    print(f"Data source: {source}")

    # Check source tables exist
    if not args.skip_source_check:
        print("Checking source tables against existing CDL/BL mappings...")
        checker = SourceTableChecker(args.mappings_root)
        results = checker.check_requirements(requirements)

        found = [r for r in results if r.found]
        missing = [r for r in results if not r.found]

        if found:
            print(f"  {len(found)} source table(s) verified:")
            for r in found:
                print(f"    [OK] {r.source_table}")

        if missing:
            print(f"  {len(missing)} source table(s) NOT FOUND:")
            for r in missing:
                hint = f" (did you mean '{r.suggestion}'?)" if r.suggestion else ""
                print(f"    [MISSING] {r.source_table}{hint}")

            # Prompt user for JSON schema samples of missing tables
            table_schemas = requirements.metadata.setdefault("missing_table_schemas", {})
            for r in missing:
                print(f"\nTable '{r.source_table}' is not found in existing mappings.")
                print("Provide a JSON sample of the table schema (column names, types, sample row).")
                print("Paste JSON and press Enter on an empty line to finish, or press Enter immediately to abort:")
                lines = []
                while True:
                    try:
                        line = input()
                    except EOFError:
                        break
                    if line == "" and not lines:
                        # Immediate empty input → abort
                        break
                    if line == "" and lines:
                        # Empty line after content → done
                        break
                    lines.append(line)

                raw_json = "\n".join(lines).strip()
                if not raw_json:
                    print(f"No schema provided for '{r.source_table}'. Aborting.", file=sys.stderr)
                    return 1

                try:
                    schema_data = json.loads(raw_json)
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON for '{r.source_table}': {e}", file=sys.stderr)
                    print("Only valid JSON input is accepted. Aborting.", file=sys.stderr)
                    return 1

                table_schemas[r.source_table] = schema_data
                print(f"  [OK] Schema accepted for '{r.source_table}'")

            print(f"\n{len(table_schemas)} missing table schema(s) provided by user. Proceeding...")

        if not results:
            print("  No source tables to check.")

    # Resolve output directory
    if args.output:
        output_dir = Path(args.output)
    else:
        output_dir = Path(args.mappings_root) / LAYERS[layer]["subfolder"] / source
    print(f"Output directory: {output_dir}")

    # Generate
    generator_cls = GENERATORS[layer]
    generator = generator_cls(api_key=args.api_key, mappings_root=args.mappings_root)

    print(f"Generating {layer} mapping files using Claude API...")
    print(f"Tables to process: {len(requirements.table_mappings)}")

    files = generator.generate(requirements)

    if not files:
        print("Warning: No files were generated", file=sys.stderr)
        return 1

    # Validate generated files
    validator = MappingValidator()
    all_valid = True
    for filename, content in files.items():
        errors = validator.validate(content, layer)
        error_count = sum(1 for e in errors if e.severity == "error")
        if error_count > 0:
            print(f"\nValidation errors in {filename}:")
            for err in errors:
                print(f"  {err}")
            all_valid = False
        else:
            warnings = [e for e in errors if e.severity == "warning"]
            if warnings:
                print(f"\n{filename}: {len(warnings)} warning(s)")
                for w in warnings:
                    print(f"  {w}")

    # Output
    if args.dry_run:
        for filename, content in files.items():
            print(f"\n{'='*60}")
            print(f"FILE: {filename}")
            print(f"{'='*60}")
            print(content)
    else:
        written = generator.write_output(output_dir, files)
        print(f"\nGenerated {len(written)} file(s):")
        for f in written:
            print(f"  {f}")

    if not all_valid:
        print("\nSome files have validation errors. Please review before using.")

    return 0


def run_server(args: argparse.Namespace) -> int:
    """Launch the FastAPI web UI."""
    import uvicorn
    print(f"Starting Smart DTC Mapping Generator web UI on http://localhost:{args.port}")
    uvicorn.run("mapping_generator.web:app", host="0.0.0.0", port=args.port, reload=False)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.serve:
        return run_server(args)
    elif args.validate:
        return run_validation(args)
    else:
        return run_generation(args)


if __name__ == "__main__":
    sys.exit(main())
