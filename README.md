# Smart DTC Mapping Generator Pipeline

A Python-based pipeline that generates YAML mapping files for BigQuery ETL pipelines. Uses the Claude API for intelligent mapping generation from CSV/PDF requirements documents. Includes a synthetic data pipeline for demo and testing purposes.

## Architecture

### Pipeline Flow

```
RAW (Src_NovaStar)  →  CDL (CDL_NovaStar)  →  BL (Business_Logic)
   automated              automated              user-driven
   JSONL → BQ load        MERGE SQL transforms   requirements CSV/PDF upload
                                                  → mapping generator → BL SQL
```

### Three Data Layers

| Layer | Dataset | How Populated | Tables |
|-------|---------|---------------|--------|
| **RAW** | `Src_NovaStar` | Automated: JSONL load via executor | 9 tables (company, customer, product, price, cost, order_line, return, demand_forecast, sales_forecast) |
| **CDL** | `CDL_NovaStar` | Automated: MERGE SQL transforms from RAW | 5 dimensions + 6 facts |
| **BL** | `Business_Logic` | User uploads requirements CSV/PDF → mapping generator produces SQL → executed against BigQuery | Generated dynamically |

## Project Structure

```
generator-pipeline/
├── mapping_generator/          # Core mapping generator (CLI + Web UI)
│   ├── cli.py                  # Entry point
│   ├── config.py               # API keys, model config
│   ├── web.py                  # FastAPI web app
│   ├── generators/             # Layer-specific generators (CDL, BL, BR)
│   ├── parsers/                # CSV, PDF, merge parsers
│   └── static/index.html       # Web UI
├── synthetic_data/             # Synthetic data pipeline
│   ├── generate.py             # Data generator (NovaStar Brands Corp)
│   ├── executor.py             # BigQuery executor (RAW → CDL pipeline)
│   ├── sql/
│   │   ├── ddl/                # DDL for RAW and CDL layers
│   │   └── transforms/         # RAW → CDL MERGE transforms
│   └── output/raw/             # Generated CSV/JSONL files
├── Dockerfile                  # Python 3.12-slim, serves on port 8080
├── cloudbuild.yaml             # Cloud Build CI/CD config
├── deploy.sh                   # One-command GCP deployment
└── requirements.txt            # Python dependencies
```

## Prerequisites

- Python 3.12+
- An [Anthropic API key](https://console.anthropic.com/)
- Google Cloud SDK (for BigQuery execution and deployment)
- A GCP project with BigQuery enabled

## Setup

1. **Clone the repository and install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

2. **Set environment variables:**

   ```bash
   export ANTHROPIC_API_KEY="your-api-key"
   export GCP_PROJECT_ID="your-gcp-project"       # required for BigQuery execution
   ```

## Live Demo

The app is deployed on Google Compute Engine:
**http://34.61.110.56:8080**

## Usage

### Web UI

Start the local web server:

```bash
python -m mapping_generator.web
```

The UI will be available at `http://localhost:8000`. Upload a requirements CSV or PDF to generate YAML mapping files. When `GCP_PROJECT_ID` is set, an "Execute on BigQuery" button allows direct execution of generated SQL.

### CLI

```bash
python -m mapping_generator.cli
```

### Synthetic Data Pipeline

Generate synthetic demo data (NovaStar Brands Corp):

```bash
# Generate JSONL files
python -m synthetic_data.generate --format jsonl --output-dir synthetic_data/output

# Run the full pipeline against BigQuery
python -m synthetic_data.executor --project $GCP_PROJECT_ID --step all
```

Individual pipeline steps are also available:

```bash
python -m synthetic_data.executor --project $GCP_PROJECT_ID --step setup-raw
python -m synthetic_data.executor --project $GCP_PROJECT_ID --step load-raw
python -m synthetic_data.executor --project $GCP_PROJECT_ID --step setup-cdl
python -m synthetic_data.executor --project $GCP_PROJECT_ID --step transform-cdl
python -m synthetic_data.executor --project $GCP_PROJECT_ID --step verify
```

## Deployment

The application deploys to Google Cloud Run via Cloud Build:

```bash
./deploy.sh $GCP_PROJECT_ID
```

### GCP Services Used

| Service | Purpose |
|---------|---------|
| **Cloud Run** | Hosts the FastAPI web UI |
| **Artifact Registry** | Docker image storage |
| **Cloud Build** | Container image builds |
| **Secret Manager** | Stores `ANTHROPIC_API_KEY` |
| **BigQuery** | Data warehouse (RAW, CDL, BL datasets) |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | Yes | API key for Claude-based mapping generation |
| `GCP_PROJECT_ID` | For BQ execution | Google Cloud project ID |
| `LLM_PROVIDER` | No | `"anthropic"` (default) or `"gemini"` |
| `GEMINI_API_KEY` | If using Gemini | API key for Gemini fallback |
| `SMART_DTC_MAPPINGS_ROOT` | No | Path to existing Mappings folder for few-shot examples |

## Key Technical Details

- Surrogate keys use `FARM_FINGERPRINT()` for hash-based INT64 keys
- All BL tables include standard ADMIN columns (configured in `config.py`)
- BL SQL uses Jinja2 template variables: `{{ target_project }}`, `{{ source_projects[0] }}`
- CDL layer uses MERGE statements for incremental upserts
- Web UI runs on port 8080 (Cloud Run) / 8000 (local development)

## Synthetic Data: NovaStar Brands Corp

A fictional consumer goods company used for demo and testing:

- 10 subsidiaries across EMEA, Americas, APAC, and LATAM
- 8 product lines (HeroForce, DreamLine, TurboTrack, etc.)
- 4 selling methods (Wholesale, DTC, E-Commerce, Marketplace)
- ~227K rows of synthetic data covering 2024-2025
