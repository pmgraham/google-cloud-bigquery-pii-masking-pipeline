#!/bin/bash
#
# Creates Pub/Sub topic and subscription for the PII masking pipeline
#

set -euo pipefail

# --- CONFIGURATION ---
PROJECT_ID="${PROJECT_ID:?ERROR: PROJECT_ID environment variable is required}"
REGION="${REGION:-us-central1}"

# Main topic/subscription for CQ -> Dataflow
TOPIC_NAME="${TOPIC_NAME:-pii-logs-topic}"
SUBSCRIPTION_NAME="${SUBSCRIPTION_NAME:-pii-logs-subscription}"

# Dead letter topic for failed records
DLQ_TOPIC_NAME="${DLQ_TOPIC_NAME:-pii-logs-dlq}"
DLQ_SUBSCRIPTION_NAME="${DLQ_SUBSCRIPTION_NAME:-pii-logs-dlq-subscription}"

# --- FUNCTIONS ---
create_topic() {
    local topic=$1
    echo "Creating topic: $topic"
    if gcloud pubsub topics describe "$topic" --project="$PROJECT_ID" &>/dev/null; then
        echo "  Topic $topic already exists, skipping."
    else
        gcloud pubsub topics create "$topic" \
            --project="$PROJECT_ID" \
            --message-retention-duration="7d"
        echo "  Topic $topic created."
    fi
}

create_subscription() {
    local subscription=$1
    local topic=$2
    local ack_deadline=${3:-600}
    local exactly_once=${4:-false}

    echo "Creating subscription: $subscription"
    if gcloud pubsub subscriptions describe "$subscription" --project="$PROJECT_ID" &>/dev/null; then
        echo "  Subscription $subscription already exists, skipping."
    else
        local cmd="gcloud pubsub subscriptions create $subscription \
            --project=$PROJECT_ID \
            --topic=$topic \
            --ack-deadline=$ack_deadline \
            --message-retention-duration=7d \
            --expiration-period=never"

        if [ "$exactly_once" = "true" ]; then
            cmd="$cmd --enable-exactly-once-delivery"
        fi

        eval $cmd
        echo "  Subscription $subscription created."
    fi
}

# --- MAIN ---
echo "=== Pub/Sub Setup for PII Masking Pipeline ==="
echo "Project: $PROJECT_ID"
echo ""

# Create main topic and subscription
# Enable exactly-once delivery to prevent duplicate message processing
create_topic "$TOPIC_NAME"
create_subscription "$SUBSCRIPTION_NAME" "$TOPIC_NAME" 600 true

echo ""

# Create dead letter topic and subscription
create_topic "$DLQ_TOPIC_NAME"
create_subscription "$DLQ_SUBSCRIPTION_NAME" "$DLQ_TOPIC_NAME" 600 false

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Resources created:"
echo "  Topic:        projects/$PROJECT_ID/topics/$TOPIC_NAME"
echo "  Subscription: projects/$PROJECT_ID/subscriptions/$SUBSCRIPTION_NAME"
echo "  DLQ Topic:    projects/$PROJECT_ID/topics/$DLQ_TOPIC_NAME"
echo "  DLQ Sub:      projects/$PROJECT_ID/subscriptions/$DLQ_SUBSCRIPTION_NAME"
