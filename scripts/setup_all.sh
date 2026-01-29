#!/bin/bash
#
# Master Setup Script for BigQuery PII Masking Pipeline
#
# This script orchestrates the complete setup of the PII masking pipeline.
# It must be run in order as there are dependencies between steps.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - bq CLI installed
#   - uv Python package manager installed
#   - Appropriate GCP permissions (Owner or Editor recommended)
#
# Usage:
#   ./scripts/setup_all.sh
#
# Environment Variables:
#   PROJECT_ID          - GCP project ID (required)
#   SOURCE_DATASET      - BigQuery dataset containing source table (required)
#   SOURCE_TABLE        - Source table name for continuous query (required)
#   BQ_OUTPUT_TABLE     - Destination table for masked data (required)
#   REGION              - Dataflow region (default: us-central1)
#   BQ_LOCATION         - BigQuery dataset location (default: US)
#   USER_EMAIL          - Your email for service account impersonation
#

set -euo pipefail

# --- CONFIGURATION ---
PROJECT_ID="${PROJECT_ID:?ERROR: PROJECT_ID environment variable is required}"
SOURCE_DATASET="${SOURCE_DATASET:?ERROR: SOURCE_DATASET environment variable is required}"
SOURCE_TABLE="${SOURCE_TABLE:?ERROR: SOURCE_TABLE environment variable is required}"
BQ_OUTPUT_TABLE="${BQ_OUTPUT_TABLE:?ERROR: BQ_OUTPUT_TABLE environment variable is required}"
REGION="${REGION:-us-central1}"
BQ_LOCATION="${BQ_LOCATION:-US}"
USER_EMAIL="${USER_EMAIL:-}"

# Pub/Sub names (can be overridden)
TOPIC_NAME="${TOPIC_NAME:-pii-logs-topic}"
SUBSCRIPTION_NAME="${SUBSCRIPTION_NAME:-pii-logs-subscription}"
DLQ_TOPIC_NAME="${DLQ_TOPIC_NAME:-pii-logs-dlq}"

# Derived variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- HELPER FUNCTIONS ---
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  STEP $1: $2${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "$1 is required but not installed."
        exit 1
    fi
}

confirm_proceed() {
    if [ "${AUTO_APPROVE:-false}" = "true" ]; then
        return 0
    fi
    read -p "Proceed? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_warn "Aborted by user."
        exit 1
    fi
}

# --- PRE-FLIGHT CHECKS ---
preflight_checks() {
    log_step "0" "Pre-flight Checks"

    log_info "Checking required tools..."
    check_command gcloud
    check_command bq
    check_command uv
    check_command gsutil

    log_info "Checking gcloud authentication..."
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -1 > /dev/null; then
        log_error "Not authenticated with gcloud. Run: gcloud auth login"
        exit 1
    fi

    ACTIVE_ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -1)
    log_info "Authenticated as: $ACTIVE_ACCOUNT"

    if [ -z "$USER_EMAIL" ]; then
        USER_EMAIL="$ACTIVE_ACCOUNT"
        log_info "Using $USER_EMAIL for service account permissions"
    fi

    log_info "Project: $PROJECT_ID"
    log_info "Source table: $PROJECT_ID.$SOURCE_DATASET.$SOURCE_TABLE"
    log_info "Output table: $PROJECT_ID.$SOURCE_DATASET.$BQ_OUTPUT_TABLE"
    log_info "Dataflow Region: $REGION"
    log_info "BigQuery Location: $BQ_LOCATION"

    log_success "Pre-flight checks passed!"
}

# --- STEP 1: Enable APIs ---
enable_apis() {
    log_step "1" "Enable Required APIs"

    log_info "Enabling APIs (this may take a minute)..."
    gcloud services enable \
        bigquery.googleapis.com \
        bigqueryreservation.googleapis.com \
        bigqueryconnection.googleapis.com \
        pubsub.googleapis.com \
        dataflow.googleapis.com \
        dlp.googleapis.com \
        storage.googleapis.com \
        compute.googleapis.com \
        --project="$PROJECT_ID"

    log_success "APIs enabled!"
}

# --- STEP 2: Get Service Account Info ---
get_service_accounts() {
    log_step "2" "Get Service Account Information"

    PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
    COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
    PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"
    CQ_SA="bq-continuous-query@${PROJECT_ID}.iam.gserviceaccount.com"

    log_info "Project Number: $PROJECT_NUMBER"
    log_info "Compute SA: $COMPUTE_SA"
    log_info "Pub/Sub SA: $PUBSUB_SA"
    log_info "CQ SA: $CQ_SA"

    # Export for use in other functions
    export PROJECT_NUMBER COMPUTE_SA PUBSUB_SA CQ_SA

    log_success "Service account info retrieved!"
}

# --- STEP 3: Grant IAM Permissions ---
grant_iam_permissions() {
    log_step "3" "Grant IAM Permissions"

    log_info "Granting permissions to Compute Engine service account..."

    # Dataflow worker role
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$COMPUTE_SA" \
        --role="roles/dataflow.worker" \
        --quiet

    # Pub/Sub subscriber
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$COMPUTE_SA" \
        --role="roles/pubsub.subscriber" \
        --quiet

    # Pub/Sub publisher (for DLQ)
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$COMPUTE_SA" \
        --role="roles/pubsub.publisher" \
        --quiet

    # Pub/Sub viewer
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$COMPUTE_SA" \
        --role="roles/pubsub.viewer" \
        --quiet

    # BigQuery data editor
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$COMPUTE_SA" \
        --role="roles/bigquery.dataEditor" \
        --quiet

    # DLP user
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$COMPUTE_SA" \
        --role="roles/dlp.user" \
        --quiet

    log_info "Creating Continuous Query service account..."

    # Create CQ service account (ignore error if exists)
    gcloud iam service-accounts create bq-continuous-query \
        --project="$PROJECT_ID" \
        --display-name="BigQuery Continuous Query Service Account" 2>/dev/null || true

    log_info "Granting permissions to CQ service account..."

    # CQ SA permissions
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$CQ_SA" \
        --role="roles/pubsub.publisher" \
        --quiet

    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$CQ_SA" \
        --role="roles/pubsub.viewer" \
        --quiet

    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$CQ_SA" \
        --role="roles/bigquery.dataViewer" \
        --quiet

    # Grant user ability to impersonate CQ SA
    gcloud iam service-accounts add-iam-policy-binding "$CQ_SA" \
        --project="$PROJECT_ID" \
        --member="user:$USER_EMAIL" \
        --role="roles/iam.serviceAccountUser" \
        --quiet

    log_success "IAM permissions granted!"
}

# --- STEP 4: Create Pub/Sub Resources ---
create_pubsub() {
    log_step "4" "Create Pub/Sub Resources"

    export PROJECT_ID
    export TOPIC_NAME SUBSCRIPTION_NAME DLQ_TOPIC_NAME
    "$SCRIPT_DIR/create_pubsub.sh"

    log_success "Pub/Sub resources created!"
}

# --- STEP 5: Create BigQuery Reservation ---
create_bq_reservation() {
    log_step "5" "Create BigQuery Reservation"

    export PROJECT_ID
    export REGION="$BQ_LOCATION"  # Reservation must match dataset location
    "$SCRIPT_DIR/create_bq_reservation.sh"

    log_success "BigQuery reservation created!"
}

# --- STEP 6: Create DLQ Storage ---
create_dlq_storage() {
    log_step "6" "Create DLQ Cloud Storage"

    export PROJECT_ID
    export REGION="$BQ_LOCATION"
    "$SCRIPT_DIR/create_dlq_storage.sh"

    log_success "DLQ storage created!"
}

# --- STEP 7: Create Dataflow Temp Bucket ---
create_dataflow_bucket() {
    log_step "7" "Create Dataflow Temp Bucket"

    BUCKET="${PROJECT_ID}-dataflow-temp"

    log_info "Creating bucket: gs://$BUCKET"
    if gsutil ls -b "gs://$BUCKET" &>/dev/null; then
        log_info "Bucket already exists, skipping creation."
    else
        gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://$BUCKET"
    fi

    log_info "Granting Compute SA access to bucket..."
    gsutil iam ch "serviceAccount:$COMPUTE_SA:roles/storage.objectAdmin" "gs://$BUCKET"

    log_success "Dataflow temp bucket ready!"
}

# --- STEP 8: Install Python Dependencies ---
install_dependencies() {
    log_step "8" "Install Python Dependencies"

    cd "$PROJECT_ROOT"
    log_info "Running uv sync..."
    uv sync

    log_success "Dependencies installed!"
}

# --- STEP 9: Deploy Dataflow Pipeline ---
deploy_dataflow() {
    log_step "9" "Deploy Dataflow Pipeline"

    export PROJECT_ID REGION
    export BQ_DATASET="$SOURCE_DATASET"
    export BQ_OUTPUT_TABLE
    export SUBSCRIPTION_NAME DLQ_TOPIC_NAME
    cd "$PROJECT_ROOT"
    uv run "$SCRIPT_DIR/deploy_dataflow.sh"

    log_info "Waiting for Dataflow job to start..."
    sleep 30

    # Check job status
    JOB_STATE=$(gcloud dataflow jobs list \
        --project="$PROJECT_ID" \
        --region="$REGION" \
        --filter="name:pii-masking-pipeline" \
        --limit=1 \
        --format="value(state)" 2>/dev/null || echo "UNKNOWN")

    if [ "$JOB_STATE" = "Running" ]; then
        log_success "Dataflow pipeline is running!"
    else
        log_warn "Dataflow job state: $JOB_STATE (may still be starting)"
    fi
}

# --- STEP 10: Start Continuous Query ---
start_continuous_query() {
    log_step "10" "Start Continuous Query"

    # First check if any CQs are already running
    log_info "Checking for existing continuous queries..."
    RUNNING_CQS=$(bq query --project_id="$PROJECT_ID" --use_legacy_sql=false --format=csv \
        "SELECT COUNT(*) FROM \`$PROJECT_ID.region-$BQ_LOCATION.INFORMATION_SCHEMA.JOBS\`
         WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
         AND continuous IS TRUE AND state = 'RUNNING'" 2>/dev/null | tail -1)

    if [ "$RUNNING_CQS" != "0" ]; then
        log_warn "There are $RUNNING_CQS continuous queries already running!"
        log_warn "Please cancel them before starting a new one to avoid duplicates."
        log_info "Use this query to see running CQs:"
        echo "  SELECT job_id, start_time FROM \`$PROJECT_ID.region-$BQ_LOCATION.INFORMATION_SCHEMA.JOBS\`"
        echo "  WHERE continuous IS TRUE AND state = 'RUNNING'"
        return 1
    fi

    log_info "Starting continuous query..."
    export PROJECT_ID
    export REGION="$BQ_LOCATION"
    export SOURCE_DATASET SOURCE_TABLE
    export TOPIC_NAME
    "$SCRIPT_DIR/create_continuous_query.sh" &

    sleep 10

    # Verify CQ started
    RUNNING_CQS=$(bq query --project_id="$PROJECT_ID" --use_legacy_sql=false --format=csv \
        "SELECT COUNT(*) FROM \`$PROJECT_ID.region-$BQ_LOCATION.INFORMATION_SCHEMA.JOBS\`
         WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
         AND continuous IS TRUE AND state = 'RUNNING'" 2>/dev/null | tail -1)

    if [ "$RUNNING_CQS" = "1" ]; then
        log_success "Continuous query started!"
    else
        log_warn "Could not verify CQ status. Check manually."
    fi
}

# --- FINAL SUMMARY ---
print_summary() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  SETUP COMPLETE!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "Pipeline components:"
    echo "  - Pub/Sub topic: $TOPIC_NAME"
    echo "  - Pub/Sub subscription: $SUBSCRIPTION_NAME (exactly-once delivery)"
    echo "  - DLQ topic: $DLQ_TOPIC_NAME"
    echo "  - BigQuery reservation: pii-pipeline-reservation-${BQ_LOCATION,,}"
    echo "  - Dataflow job: pii-masking-pipeline"
    echo "  - DLQ archive: gs://${PROJECT_ID}-dlq-archive"
    echo ""
    echo "To test the pipeline:"
    echo "  1. Insert test records into: $PROJECT_ID.$SOURCE_DATASET.$SOURCE_TABLE"
    echo "  2. Wait 15-30 seconds for processing"
    echo "  3. Check masked table:"
    echo "     bq query --use_legacy_sql=false \\"
    echo "       \"SELECT COUNT(*) FROM \\\`$PROJECT_ID.$SOURCE_DATASET.$BQ_OUTPUT_TABLE\\\`\""
    echo ""
    echo "Monitoring commands:"
    echo "  - Check Dataflow: gcloud dataflow jobs list --project=$PROJECT_ID --region=$REGION"
    echo "  - Check CQ: See README.md for INFORMATION_SCHEMA query"
    echo ""
}

# --- MAIN ---
main() {
    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  BigQuery PII Masking Pipeline - Master Setup Script       ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    preflight_checks

    echo ""
    log_info "This script will set up the complete PII masking pipeline."
    log_info "Estimated time: 5-10 minutes"
    echo ""
    confirm_proceed

    enable_apis
    get_service_accounts
    grant_iam_permissions
    create_pubsub
    create_bq_reservation
    create_dlq_storage
    create_dataflow_bucket
    install_dependencies
    deploy_dataflow
    start_continuous_query

    print_summary
}

# Run main function
main "$@"
