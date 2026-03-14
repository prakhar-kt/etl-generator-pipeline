#!/bin/bash
# ============================================================
# NovaStar Brands Corp - GCP Deployment Script
# Deploys the mapping generator + data pipeline to GCP
# ============================================================

set -e

PROJECT_ID="${1:-m-mapping-gen-2026}"
REGION="us-central1"
REPO_NAME="mapping-generator"
IMAGE_NAME="app"
SERVICE_NAME="mapping-generator"

# Add gcloud to PATH
export PATH="/opt/homebrew/share/google-cloud-sdk/bin:$PATH"

echo "============================================================"
echo "Deploying NovaStar Pipeline to GCP"
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "============================================================"

# ─── Step 1: Enable APIs ─────────────────────────────────────
echo ""
echo "Step 1: Enabling required APIs..."
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  bigquery.googleapis.com \
  --project="$PROJECT_ID" 2>/dev/null || true

# ─── Step 2: Create Artifact Registry repo ───────────────────
echo ""
echo "Step 2: Creating Artifact Registry repository..."
gcloud artifacts repositories create "$REPO_NAME" \
  --repository-format=docker \
  --location="$REGION" \
  --project="$PROJECT_ID" 2>/dev/null || echo "  Repository already exists"

gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

# ─── Step 3: Build and push container ─────────────────────────
echo ""
echo "Step 3: Building and pushing container image..."
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:latest"

gcloud builds submit \
  --tag "$IMAGE_URI" \
  --project="$PROJECT_ID" \
  --quiet

# ─── Step 4: Deploy to Cloud Run ─────────────────────────────
echo ""
echo "Step 4: Deploying to Cloud Run..."
gcloud run deploy "$SERVICE_NAME" \
  --image "$IMAGE_URI" \
  --region "$REGION" \
  --platform managed \
  --memory 1Gi \
  --timeout 300 \
  --allow-unauthenticated \
  --set-env-vars "GCP_PROJECT_ID=${PROJECT_ID},LLM_PROVIDER=anthropic" \
  --set-secrets "ANTHROPIC_API_KEY=ANTHROPIC_API_KEY:latest" \
  --project="$PROJECT_ID"

# ─── Step 5: Generate synthetic data ─────────────────────────
echo ""
echo "Step 5: Generating synthetic data..."
python -m synthetic_data.generate --format jsonl --output-dir synthetic_data/output --seed 42

# ─── Step 6: Run pipeline (RAW → CDL) ────────────────────────
echo ""
echo "Step 6: Running data pipeline (RAW → CDL)..."
pip install google-cloud-bigquery --quiet 2>/dev/null
python -m synthetic_data.executor --project "$PROJECT_ID" --step all

# ─── Done ─────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "Deployment complete!"
echo ""
SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" \
  --region "$REGION" \
  --project="$PROJECT_ID" \
  --format="value(status.url)")
echo "Web UI: $SERVICE_URL"
echo ""
echo "BigQuery datasets:"
echo "  RAW: ${PROJECT_ID}.Src_NovaStar"
echo "  CDL: ${PROJECT_ID}.CDL_NovaStar"
echo "  BL:  (generated via web UI with requirements upload)"
echo "============================================================"
