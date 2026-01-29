#!/bin/bash
#
# Creates BigQuery reservation for Continuous Queries
# Continuous Queries require Enterprise or Enterprise Plus edition
#

set -euo pipefail

# --- CONFIGURATION ---
PROJECT_ID="${PROJECT_ID:?ERROR: PROJECT_ID environment variable is required}"
REGION="${REGION:-US}"

# Reservation settings
RESERVATION_NAME="${RESERVATION_NAME:-pii-pipeline-reservation-us}"
BASELINE_SLOTS="${BASELINE_SLOTS:-50}"
AUTOSCALE_MAX_SLOTS="${AUTOSCALE_MAX_SLOTS:-250}"
EDITION="${EDITION:-ENTERPRISE}"  # ENTERPRISE or ENTERPRISE_PLUS

# Assignment settings
ASSIGNMENT_NAME="${ASSIGNMENT_NAME:-pii-pipeline-assignment}"

# --- FUNCTIONS ---
create_reservation() {
    echo "Creating reservation: $RESERVATION_NAME"
    if bq show --reservation --project_id="$PROJECT_ID" --location="$REGION" "$RESERVATION_NAME" &>/dev/null; then
        echo "  Reservation $RESERVATION_NAME already exists, skipping."
    else
        bq mk \
            --project_id="$PROJECT_ID" \
            --location="$REGION" \
            --reservation \
            --slots="$BASELINE_SLOTS" \
            --autoscale_max_slots="$AUTOSCALE_MAX_SLOTS" \
            --edition="$EDITION" \
            "$RESERVATION_NAME"
        echo "  Reservation $RESERVATION_NAME created."
    fi
}

create_assignment() {
    echo "Creating assignment: $ASSIGNMENT_NAME"

    # Check if assignment already exists
    existing=$(bq ls --project_id="$PROJECT_ID" --location="$REGION" --reservation_assignment --reservation_id="$RESERVATION_NAME" 2>/dev/null | grep -c "$PROJECT_ID" || true)

    if [[ "$existing" -gt 0 ]]; then
        echo "  Assignment for project already exists, skipping."
    else
        bq mk \
            --project_id="$PROJECT_ID" \
            --location="$REGION" \
            --reservation_assignment \
            --reservation_id="$RESERVATION_NAME" \
            --job_type=CONTINUOUS \
            --assignee_type=PROJECT \
            --assignee_id="$PROJECT_ID"
        echo "  Assignment created."
    fi
}

# --- MAIN ---
echo "=== BigQuery Reservation Setup for Continuous Queries ==="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Edition: $EDITION"
echo "Baseline slots: $BASELINE_SLOTS"
echo "Autoscale max slots: $AUTOSCALE_MAX_SLOTS"
echo ""

create_reservation
echo ""

create_assignment
echo ""

echo "=== Setup Complete ==="
echo ""
echo "Reservation: $RESERVATION_NAME"
echo "Baseline slots: $BASELINE_SLOTS (floor)"
echo "Autoscale max: $AUTOSCALE_MAX_SLOTS"
echo "Edition: $EDITION"
echo ""
echo "To view reservation:"
echo "  bq show --reservation --project_id=$PROJECT_ID --location=$REGION $RESERVATION_NAME"
echo ""
echo "To delete reservation (when no longer needed):"
echo "  bq rm --reservation --project_id=$PROJECT_ID --location=$REGION $RESERVATION_NAME"
