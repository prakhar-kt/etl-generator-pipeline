# Smart DTC Mapping Generator

## Overview
CLI tool that generates YAML mapping files for Mattel's Smart DTC BigQuery ETL pipeline. Takes CSV/Excel/PDF requirements as input and produces layer-specific YAML mapping files using the Claude API for intelligent generation.

## Quick Start
```bash
# Set API key
export ANTHROPIC_API_KEY="..."

# Generate from CSV
python -m mapping_generator --input requirements.csv --layer CDL --source PRP1

# Generate from PDF
python -m mapping_generator --input spec.pdf --layer BL --source SHOPIFY_MC

# Validate existing mapping
python -m mapping_generator --validate path/to/mapping.yml --layer BL

# Dry run (stdout only)
python -m mapping_generator --input reqs.csv --layer CDL --source PRP1 --dry-run
```

## Architecture

### Pipeline Flow
```
Input (CSV/Excel/PDF) -> Parser -> ParsedRequirements -> Generator -> YAML files -> Validator
```

### Key Modules

| Module | Purpose |
|--------|---------|
| `cli.py` | Entry point, argument parsing, orchestration |
| `config.py` | API keys, model config, layer definitions, ADMIN columns |
| `parsers/csv_parser.py` | CSV/Excel parsing with flexible column aliases |
| `parsers/pdf_parser.py` | PDF text extraction + Claude API interpretation |
| `generators/base.py` | Shared logic: Claude API calls, few-shot example loading, ADMIN columns |
| `generators/cdl_generator.py` | CDL (RAW -> CDL) dataflow YAML generation |
| `generators/cleansed_raw_generator.py` | Cleansed_RAW dataflow YAML generation |
| `generators/bl_generator.py` | BL (CDL -> BL) SQL mapping with MERGE/DDL |
| `generators/br_generator.py` | BR (BL -> BR) SQL mapping for reporting |
| `source_checker.py` | Validates source tables exist in existing mappings |
| `validator.py` | Schema validation for generated YAML files |

### Data Layers

| Layer | Style | Stage | Description |
|-------|-------|-------|-------------|
| **CDL** | `dataflow` list | RAW to CDL | Cleansed Data Layer - field-level source-to-target mappings |
| **Cleansed_RAW** | `dataflow` list | RAW to Cleansed | Similar to CDL, targets cleansed datasets |
| **BL** | SQL (MERGE) | CDL to BL | Business Logic - CREATE TABLE + MERGE SQL with Jinja2 templates |
| **BR** | SQL (MERGE) | BL to BR | Business Reporting - complex aggregations and cross-table joins |

### Data Models (in `parsers/csv_parser.py`)

- **`FieldMapping`**: Single column mapping (source_col, target_col, data_type, is_key, transform, foreign_key_ref, value)
- **`TableMapping`**: Table-level mapping with fields, primary_keys, foreign_keys, incremental/partition fields
- **`ParsedRequirements`**: Top-level container with source_name, layer, table_mappings, raw_text, metadata

### YAML Output Formats

**CDL/Cleansed_RAW** (dataflow style):
```yaml
dataflow:
  - source_dataset: Src_prp1
    source_table: table_name
    target_dataset: CDL_Facts
    target_table: cdl_fact_table_name
    fields:
      - source_col: COL_A
        target_col: COL_A
        type: STRING
    primary_keys: [KEY_COL]
    foreign_keys: []
```

**BL/BR** (SQL style):
```yaml
metadata:
  stage_name: "CDL to BL"
  source_table_names: "..."
  target_table_name: "..."
create_table: |
  CREATE TABLE IF NOT EXISTS `{{ target_project }}.Dataset.table`(...)
get_max_date: |
  SELECT COALESCE(MAX(ADMIN_LOAD_DATE), ...) FROM ...
merge_statement: |
  MERGE INTO ... USING (...) ON ... WHEN MATCHED ... WHEN NOT MATCHED ...
```

### Key Conventions
- All BL/BR tables include standard ADMIN columns (see `config.py:ADMIN_COLUMNS`)
- Jinja2 template vars: `{{ target_project }}`, `{{ source_projects[0] }}`, `{{ process_id }}`
- Key columns prefixed with `KEY_` and use `FARM_FINGERPRINT` for hash generation
- Composite keys use `ADMIN_COMPOSITEKEY_HASH = FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(...)))`
- Claude model: `claude-sonnet-4-20250514`, max tokens: 8192
- Few-shot examples loaded from existing Mappings directory (max 2 per generation)
- Each generator has programmatic fallback if Claude output isn't valid YAML

## Dependencies
- `anthropic` - Claude API client
- `pypdf` - PDF text extraction
- `pyyaml` - YAML parsing/generation
- `pandas` / `openpyxl` - CSV/Excel reading

## Environment Variables
- `ANTHROPIC_API_KEY` - Required for generation
- `SMART_DTC_MAPPINGS_ROOT` - Path to existing Mappings folder (for few-shot examples and source checking)
