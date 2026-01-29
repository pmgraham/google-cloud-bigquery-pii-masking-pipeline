#!/bin/bash
#
# Creates BigQuery Continuous Query to export data to Pub/Sub
#

set -euo pipefail

# --- CONFIGURATION ---
PROJECT_ID="${PROJECT_ID:?ERROR: PROJECT_ID environment variable is required}"
REGION="${REGION:-US}"

# Source table (required)
SOURCE_DATASET="${SOURCE_DATASET:?ERROR: SOURCE_DATASET environment variable is required}"
SOURCE_TABLE="${SOURCE_TABLE:?ERROR: SOURCE_TABLE environment variable is required}"

# Pub/Sub topic for export
TOPIC_NAME="${TOPIC_NAME:-pii-logs-topic}"

# Service account for Pub/Sub export (derived from PROJECT_ID)
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-bq-continuous-query@${PROJECT_ID}.iam.gserviceaccount.com}"

# --- DERIVED VARIABLES ---
FULL_TOPIC_URI="https://pubsub.googleapis.com/projects/${PROJECT_ID}/topics/${TOPIC_NAME}"
FULL_SOURCE_TABLE="${PROJECT_ID}.${SOURCE_DATASET}.${SOURCE_TABLE}"

# --- MAIN ---
echo "=== BigQuery Continuous Query Setup ==="
echo "Project: $PROJECT_ID"
echo "Source table: $FULL_SOURCE_TABLE"
echo "Target topic: $FULL_TOPIC_URI"
echo ""

echo "Starting continuous query..."
echo "NOTE: This runs as a background job. Use Ctrl+C to detach (query continues running)."
echo ""

# Run the continuous query
bq query \
    --project_id="$PROJECT_ID" \
    --use_legacy_sql=false \
    --location="$REGION" \
    --continuous=true \
    --nouse_cache \
    --connection_property="service_account=${SERVICE_ACCOUNT}" \
"EXPORT DATA
  OPTIONS (
    format = 'CLOUD_PUBSUB',
    uri = '${FULL_TOPIC_URI}'
  ) AS (
  SELECT
    TO_JSON_STRING(STRUCT(
      request,
      response,
      userIamPrincipal,
      timestamp,
      userQuery,
      serviceTextReply,
      serviceLabel,
      methodName,
      serviceAttributionToken,
      serviceName
    )) AS message
  FROM
    APPENDS(TABLE \`${FULL_SOURCE_TABLE}\`, NULL)
)"
