#!/bin/bash
# ============================================================
# Set up Cloud Scheduler to trigger daily CDL + BL refresh
# ============================================================

set -e

PROJECT_ID="${1:-m-mapping-gen-2026}"
VM_IP="34.134.87.67"
SCHEDULE="${2:-0 6 * * *}"   # Default: 6 AM UTC daily
TIMEZONE="UTC"

export PATH="/opt/homebrew/share/google-cloud-sdk/bin:$PATH"

echo "============================================================"
echo "Setting up Cloud Scheduler for daily pipeline refresh"
echo "Project:  $PROJECT_ID"
echo "Target:   http://$VM_IP:8080/refresh-pipeline"
echo "Schedule: $SCHEDULE ($TIMEZONE)"
echo "============================================================"

# ─── Step 1: Enable Cloud Scheduler API ──────────────────────
echo ""
echo "Step 1: Enabling Cloud Scheduler API..."
gcloud services enable cloudscheduler.googleapis.com \
  --project="$PROJECT_ID" 2>/dev/null || true

# ─── Step 2: Create (or update) the scheduler job ────────────
echo ""
echo "Step 2: Creating Cloud Scheduler job..."

# Delete existing job if it exists (update not supported for all fields)
gcloud scheduler jobs delete refresh-pipeline \
  --location=us-central1 \
  --project="$PROJECT_ID" \
  --quiet 2>/dev/null || true

gcloud scheduler jobs create http refresh-pipeline \
  --location=us-central1 \
  --schedule="$SCHEDULE" \
  --uri="http://$VM_IP:8080/refresh-pipeline" \
  --http-method=POST \
  --time-zone="$TIMEZONE" \
  --attempt-deadline=1800s \
  --project="$PROJECT_ID"

echo ""
echo "============================================================"
echo "Cloud Scheduler job created!"
echo ""
echo "  Job:      refresh-pipeline"
echo "  Schedule: $SCHEDULE ($TIMEZONE)"
echo "  Target:   POST http://$VM_IP:8080/refresh-pipeline"
echo "  Timeout:  30 minutes"
echo ""
echo "Commands:"
echo "  # Trigger manually:"
echo "  gcloud scheduler jobs run refresh-pipeline --location=us-central1 --project=$PROJECT_ID"
echo ""
echo "  # Check status:"
echo "  gcloud scheduler jobs describe refresh-pipeline --location=us-central1 --project=$PROJECT_ID"
echo ""
echo "  # View refresh history (via API):"
echo "  curl http://$VM_IP:8080/refresh-history"
echo "============================================================"
