# NovaStar / Smart DTC Mapping Generator Pipeline

## Project Overview
A Python-based pipeline that generates YAML mapping files for BigQuery ETL pipelines, plus a synthetic data pipeline for demo purposes. Uses Claude API for intelligent mapping generation from CSV/PDF requirements.

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
| **BL** | `Business_Logic` | Manual: user uploads requirements CSV/PDF → mapping generator produces SQL → executed against BQ | Generated dynamically |

### Synthetic Data: NovaStar Brands Corp
Fictional consumer goods company (similar structure to real data but no real references):
- 10 subsidiaries across EMEA, Americas, APAC, LATAM
- 8 product lines (HeroForce, DreamLine, TurboTrack, etc.)
- 4 selling methods (Wholesale, DTC, E-Commerce, Marketplace)
- ~227K rows of synthetic data covering 2024-2025

## Key Directories

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
│   │   ├── ddl/
│   │   │   ├── 01_raw_tables.sql    # RAW layer DDL (Src_NovaStar)
│   │   │   └── 02_cdl_tables.sql    # CDL layer DDL (CDL_NovaStar)
│   │   └── transforms/
│   │       └── 01_raw_to_cdl.sql    # RAW → CDL MERGE transforms
│   └── output/raw/             # Generated CSV/JSONL files
├── Dockerfile                  # Python 3.12-slim, serves on port 8080
├── .dockerignore
├── cloudbuild.yaml             # Cloud Build CI/CD config
├── deploy.sh                   # One-command GCP deployment
└── requirements.txt            # Python dependencies
```

## GitHub & CI/CD

### Repository
- **Repo**: https://github.com/prakhar-kt/etl-generator-pipeline (public)
- **Account**: `prakhar-kt`

### Live Web UI
https://mapping-generator-698702654413.us-central1.run.app

### Continuous Deployment
Every push to `main` automatically deploys to Cloud Run via Cloud Build:
1. Cloud Build trigger `gen-pipeline-trigger` fires on push to `main`
2. Builds Docker image and pushes to Artifact Registry
3. Deploys new revision to Cloud Run with env vars and secrets
- Trigger uses deployer service account + `REGIONAL_USER_OWNED_BUCKET` for logs
- Build console: https://console.cloud.google.com/cloud-build/builds;region=us-central1?project=m-mapping-gen-2026

## GCP Deployment

### Project: `m-mapping-gen-2026`
### Services Used:
| Service | Purpose |
|---------|---------|
| **Cloud Run** | Hosts FastAPI web UI (mapping generator) |
| **Artifact Registry** | Docker image storage (`mapping-generator` repo) |
| **Cloud Build** | Builds container images |
| **Secret Manager** | Stores `ANTHROPIC_API_KEY` |
| **BigQuery** | Data warehouse (RAW, CDL, BL datasets) |

### Authentication
- Service account: `deployer@m-mapping-gen-2026.iam.gserviceaccount.com`
- Key file: `/Users/hobbes/gcp-deployer-key.json`
- gcloud path: `/opt/homebrew/share/google-cloud-sdk/bin/gcloud`

### Deploy Commands
```bash
# Full deployment
./deploy.sh m-mapping-gen-2026

# Generate synthetic data only
python -m synthetic_data.generate --format jsonl --output-dir synthetic_data/output

# Run pipeline against BigQuery
python -m synthetic_data.executor --project m-mapping-gen-2026 --step all

# Individual steps
python -m synthetic_data.executor --project m-mapping-gen-2026 --step setup-raw
python -m synthetic_data.executor --project m-mapping-gen-2026 --step load-raw
python -m synthetic_data.executor --project m-mapping-gen-2026 --step setup-cdl
python -m synthetic_data.executor --project m-mapping-gen-2026 --step transform-cdl
python -m synthetic_data.executor --project m-mapping-gen-2026 --step verify
```

## Environment Variables
- `ANTHROPIC_API_KEY` — Required for LLM generation
- `LLM_PROVIDER` — "anthropic" (default) or "gemini"
- `GEMINI_API_KEY` — For Gemini fallback
- `GCP_PROJECT_ID` — Set on Cloud Run for BQ execution
- `SMART_DTC_MAPPINGS_ROOT` — Path to existing Mappings folder for few-shot examples

## Key Technical Patterns
- Surrogate keys use `FARM_FINGERPRINT()` for hash-based INT64 keys
- All BL tables include standard ADMIN columns (see `config.py:ADMIN_COLUMNS`)
- Jinja2 template vars in BL SQL: `{{ target_project }}`, `{{ source_projects[0] }}`
- CDL uses MERGE for incremental upserts
- Web UI on port 8080 (Cloud Run) / 8000 (local)

## Web UI BigQuery Execution
The web UI has an "Execute on BigQuery" button that appears after generating BL/BR mappings (when `GCP_PROJECT_ID` is set):
- `GET /bq-status` — checks if BigQuery is available
- `POST /execute-bl` — takes YAML content, extracts CREATE TABLE + MERGE SQL, replaces Jinja2 placeholders with actual project, and executes against BigQuery
- Shows per-step results (create_table, merge/insert, row count verification)
- Per-file and "Execute All" buttons available
