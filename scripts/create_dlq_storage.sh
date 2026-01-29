#!/bin/bash
#
# Creates Cloud Storage bucket and Pub/Sub subscription for dead letter queue archival
#

set -euo pipefail

# --- CONFIGURATION ---
PROJECT_ID="${PROJECT_ID:?ERROR: PROJECT_ID environment variable is required}"
REGION="${REGION:-US}"

# DLQ topic
DLQ_TOPIC_NAME="${DLQ_TOPIC_NAME:-pii-logs-dlq}"

# Cloud Storage settings
BUCKET_NAME="${BUCKET_NAME:-${PROJECT_ID}-dlq-archive}"
FILE_PREFIX="${FILE_PREFIX:-dlq/}"
FILE_SUFFIX="${FILE_SUFFIX:-.json}"
MAX_DURATION="${MAX_DURATION:-5m}"
MAX_BYTES="${MAX_BYTES:-10MB}"

# Subscription name
DLQ_GCS_SUBSCRIPTION="${DLQ_GCS_SUBSCRIPTION:-pii-logs-dlq-gcs}"

# --- DERIVED VARIABLES ---
PUBSUB_SA="service-$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')@gcp-sa-pubsub.iam.gserviceaccount.com"

# --- FUNCTIONS ---
create_bucket() {
    echo "Creating GCS bucket: gs://$BUCKET_NAME"
    if gsutil ls -b "gs://$BUCKET_NAME" &>/dev/null; then
        echo "  Bucket already exists, skipping."
    else
        gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://$BUCKET_NAME"
        echo "  Bucket created."
    fi
}

grant_bucket_permissions() {
    echo "Granting Pub/Sub service account access to bucket..."
    gsutil iam ch "serviceAccount:${PUBSUB_SA}:roles/storage.objectCreator" "gs://${BUCKET_NAME}"
    gsutil iam ch "serviceAccount:${PUBSUB_SA}:roles/storage.legacyBucketReader" "gs://${BUCKET_NAME}"
    echo "  Permissions granted."
}

create_gcs_subscription() {
    echo "Creating Cloud Storage subscription: $DLQ_GCS_SUBSCRIPTION"
    if gcloud pubsub subscriptions describe "$DLQ_GCS_SUBSCRIPTION" --project="$PROJECT_ID" &>/dev/null; then
        echo "  Subscription already exists, skipping."
    else
        gcloud pubsub subscriptions create "$DLQ_GCS_SUBSCRIPTION" \
            --project="$PROJECT_ID" \
            --topic="$DLQ_TOPIC_NAME" \
            --cloud-storage-bucket="$BUCKET_NAME" \
            --cloud-storage-file-prefix="$FILE_PREFIX" \
            --cloud-storage-file-suffix="$FILE_SUFFIX" \
            --cloud-storage-max-duration="$MAX_DURATION" \
            --cloud-storage-max-bytes="$MAX_BYTES"
        echo "  Subscription created."
    fi
}

# --- MAIN ---
echo "=== Dead Letter Queue Cloud Storage Setup ==="
echo "Project: $PROJECT_ID"
echo "Bucket: gs://$BUCKET_NAME"
echo "DLQ Topic: $DLQ_TOPIC_NAME"
echo ""

create_bucket
echo ""

grant_bucket_permissions
echo ""

create_gcs_subscription
echo ""

echo "=== Setup Complete ==="
echo ""
echo "Dead letter messages will be archived to:"
echo "  gs://${BUCKET_NAME}/${FILE_PREFIX}*.json"
echo ""
echo "Messages are batched and written every $MAX_DURATION or when reaching $MAX_BYTES"
