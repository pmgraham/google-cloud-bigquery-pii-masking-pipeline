#!/bin/bash
#
# Deploys the PII masking Dataflow streaming job
#

set -euo pipefail

# --- CONFIGURATION ---
PROJECT_ID="${PROJECT_ID:?ERROR: PROJECT_ID environment variable is required}"
REGION="${REGION:-us-central1}"

# Pub/Sub settings
SUBSCRIPTION_NAME="${SUBSCRIPTION_NAME:-pii-logs-subscription}"
DLQ_TOPIC_NAME="${DLQ_TOPIC_NAME:-pii-logs-dlq}"

# BigQuery settings (required)
BQ_DATASET="${BQ_DATASET:?ERROR: BQ_DATASET environment variable is required}"
BQ_OUTPUT_TABLE="${BQ_OUTPUT_TABLE:?ERROR: BQ_OUTPUT_TABLE environment variable is required}"

# Dataflow settings
JOB_NAME="${JOB_NAME:-pii-masking-pipeline}"
TEMP_BUCKET="${TEMP_BUCKET:-${PROJECT_ID}-dataflow-temp}"
MAX_WORKERS="${MAX_WORKERS:-3}"
MACHINE_TYPE="${MACHINE_TYPE:-n1-standard-2}"
NETWORK="${NETWORK:-${PROJECT_ID}}"
SUBNETWORK="${SUBNETWORK:-regions/${REGION}/subnetworks/${PROJECT_ID}}"

# DLP settings (optional templates)
DEIDENTIFY_TEMPLATE="${DEIDENTIFY_TEMPLATE:-}"
INSPECT_TEMPLATE="${INSPECT_TEMPLATE:-}"

# --- DERIVED VARIABLES ---
INPUT_SUBSCRIPTION="projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION_NAME}"
OUTPUT_TABLE="${PROJECT_ID}:${BQ_DATASET}.${BQ_OUTPUT_TABLE}"
DEAD_LETTER_TOPIC="projects/${PROJECT_ID}/topics/${DLQ_TOPIC_NAME}"
TEMP_LOCATION="gs://${TEMP_BUCKET}/temp"
STAGING_LOCATION="gs://${TEMP_BUCKET}/staging"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PIPELINE_FILE="${PROJECT_ROOT}/dataflow_pii_masking.py"
REQUIREMENTS_FILE="${PROJECT_ROOT}/requirements.txt"
PYPROJECT_FILE="${PROJECT_ROOT}/pyproject.toml"

# --- FUNCTIONS ---
generate_requirements() {
    # Generate requirements.txt from pyproject.toml for Dataflow workers
    echo "Generating requirements.txt from pyproject.toml..."
    if [[ ! -f "$PYPROJECT_FILE" ]]; then
        echo "ERROR: pyproject.toml not found: $PYPROJECT_FILE"
        exit 1
    fi

    # Extract dependencies from pyproject.toml and write to requirements.txt
    echo "# Auto-generated from pyproject.toml for Dataflow workers" > "$REQUIREMENTS_FILE"
    echo "# Do not edit manually - update pyproject.toml instead" >> "$REQUIREMENTS_FILE"
    grep -A 100 '^\s*dependencies\s*=' "$PYPROJECT_FILE" | \
        grep -E '^\s*"' | \
        sed 's/^[[:space:]]*"//; s/"[,]*$//' >> "$REQUIREMENTS_FILE"

    echo "  Generated: $REQUIREMENTS_FILE"
}

ensure_temp_bucket() {
    echo "Checking temp bucket: gs://$TEMP_BUCKET"
    if gsutil ls -b "gs://$TEMP_BUCKET" &>/dev/null; then
        echo "  Bucket exists."
    else
        echo "  Creating bucket..."
        gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://$TEMP_BUCKET"
        echo "  Bucket created."
    fi
}

check_existing_job() {
    echo "Checking for existing job: $JOB_NAME"
    local existing_job
    existing_job=$(gcloud dataflow jobs list \
        --project="$PROJECT_ID" \
        --region="$REGION" \
        --filter="name=${JOB_NAME} AND state=Running" \
        --format="value(id)" \
        2>/dev/null || true)

    if [[ -n "$existing_job" ]]; then
        echo "  WARNING: Job '$JOB_NAME' is already running (ID: $existing_job)"
        read -p "  Cancel existing job and redeploy? (y/N): " confirm
        if [[ "$confirm" =~ ^[Yy]$ ]]; then
            echo "  Cancelling existing job..."
            gcloud dataflow jobs cancel "$existing_job" \
                --project="$PROJECT_ID" \
                --region="$REGION"
            echo "  Waiting for job to stop..."
            sleep 30
        else
            echo "  Aborting deployment."
            exit 1
        fi
    fi
}

deploy_pipeline() {
    echo "Deploying Dataflow pipeline..."
    echo "  Job name: $JOB_NAME"
    echo "  Input: $INPUT_SUBSCRIPTION"
    echo "  Output: $OUTPUT_TABLE"
    echo "  DLQ: $DEAD_LETTER_TOPIC"
    echo ""

    local cmd=(
        python "$PIPELINE_FILE"
        --runner=DataflowRunner
        --project="$PROJECT_ID"
        --region="$REGION"
        --job_name="$JOB_NAME"
        --temp_location="$TEMP_LOCATION"
        --staging_location="$STAGING_LOCATION"
        --max_num_workers="$MAX_WORKERS"
        --worker_machine_type="$MACHINE_TYPE"
        --input_subscription="$INPUT_SUBSCRIPTION"
        --output_table="$OUTPUT_TABLE"
        --dead_letter_topic="$DEAD_LETTER_TOPIC"
        --dlp_project="$PROJECT_ID"
        --streaming
        --enable_streaming_engine
        --requirements_file="$REQUIREMENTS_FILE"
        --network="$NETWORK"
        --subnetwork="$SUBNETWORK"
        --no_use_public_ips
    )

    # Add service account if specified
    if [[ -n "${SERVICE_ACCOUNT:-}" ]]; then
        cmd+=(--service_account_email="$SERVICE_ACCOUNT")
    fi

    # Add optional DLP templates if specified
    if [[ -n "$DEIDENTIFY_TEMPLATE" ]]; then
        cmd+=(--deidentify_template="$DEIDENTIFY_TEMPLATE")
    fi
    if [[ -n "$INSPECT_TEMPLATE" ]]; then
        cmd+=(--inspect_template="$INSPECT_TEMPLATE")
    fi

    echo "Running: ${cmd[*]}"
    echo ""
    "${cmd[@]}"
}

# --- MAIN ---
echo "=== Dataflow PII Masking Pipeline Deployment ==="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo ""

# Pre-flight checks
if [[ ! -f "$PIPELINE_FILE" ]]; then
    echo "ERROR: Pipeline file not found: $PIPELINE_FILE"
    exit 1
fi

generate_requirements
echo ""

ensure_temp_bucket
echo ""

check_existing_job
echo ""

deploy_pipeline

echo ""
echo "=== Deployment Initiated ==="
echo ""
echo "Monitor the job at:"
echo "  https://console.cloud.google.com/dataflow/jobs/${REGION}?project=${PROJECT_ID}"
