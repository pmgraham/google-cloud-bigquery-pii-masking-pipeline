# BigQuery PII Masking Pipeline - Deployment Guide

This guide documents the complete setup process for the real-time PII masking pipeline that reads from BigQuery, masks sensitive data using Cloud DLP, and writes to a masked destination table.

## Quick Start

For a fully automated setup, run the master setup script:

```bash
# Set required environment variables
export PROJECT_ID="your-project-id"
export SOURCE_DATASET="your-dataset"
export SOURCE_TABLE="your-source-table"
export BQ_OUTPUT_TABLE="your-source-table-masked"
export USER_EMAIL="your-email@domain.com"

# Run the master setup script
./scripts/setup_all.sh
```

The script will guide you through all steps and set up the complete pipeline.

For manual setup or troubleshooting, follow the detailed steps below.

## Architecture Overview

```
┌─────────────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  Source BQ Table    │────▶│  Continuous │────▶│    Pub/Sub       │────▶│     Dataflow        │
│  (source table)     │     │    Query    │     │  pii-logs-topic  │     │  PII Masking Job    │
│                     │     │             │     │                  │     │                     │
└─────────────────────┘     └─────────────┘     └──────────────────┘     └──────────┬──────────┘
                                                                                     │
                                                        ┌────────────────────────────┼────────────────────────────┐
                                                        │                            │                            │
                                                        ▼                            ▼                            ▼
                                               ┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
                                               │  Masked BQ Table │         │   Dead Letter   │         │   Cloud Storage │
                                               │  (output table)  │         │   Queue (DLQ)   │────────▶│   DLQ Archive   │
                                               │                  │         │                 │         │                 │
                                               └─────────────────┘         └─────────────────┘         └─────────────────┘
```

## Prerequisites

- Google Cloud SDK (`gcloud`) installed and configured
- `bq` command-line tool
- `uv` Python package manager
- Python 3.9+
- Appropriate GCP project permissions (Owner or Editor recommended for initial setup)

## Setup Order (Critical!)

The setup must happen in this specific order to avoid permission and dependency issues:

1. Enable APIs
2. Get Project Number and Service Account Info
3. Grant IAM Permissions
4. Create Pub/Sub Resources
5. Create BigQuery Reservation
6. Create Cloud Storage for DLQ Archive
7. Create Dataflow Temp Bucket
8. Install Python Dependencies
9. Deploy Dataflow Pipeline
10. Start Continuous Query

---

## Step 1: Enable Required APIs

```bash
PROJECT_ID="your-project-id"

gcloud services enable \
    bigquery.googleapis.com \
    bigqueryreservation.googleapis.com \
    bigqueryconnection.googleapis.com \
    pubsub.googleapis.com \
    dataflow.googleapis.com \
    dlp.googleapis.com \
    storage.googleapis.com \
    compute.googleapis.com \
    --project=$PROJECT_ID
```

## Step 2: Get Project Number and Service Account Info

```bash
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

echo "Project Number: $PROJECT_NUMBER"
echo "Compute SA: $COMPUTE_SA"
echo "Pub/Sub SA: $PUBSUB_SA"
```

## Step 3: Grant IAM Permissions

### Compute Engine Default Service Account (Dataflow workers)

```bash
# Dataflow worker role
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$COMPUTE_SA" \
    --role="roles/dataflow.worker"

# Pub/Sub subscriber (read from subscription)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$COMPUTE_SA" \
    --role="roles/pubsub.subscriber"

# Pub/Sub publisher (write to DLQ)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$COMPUTE_SA" \
    --role="roles/pubsub.publisher"

# Pub/Sub viewer (check subscription config)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$COMPUTE_SA" \
    --role="roles/pubsub.viewer"

# BigQuery data editor (write to destination table)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$COMPUTE_SA" \
    --role="roles/bigquery.dataEditor"

# DLP user (call DLP API for masking)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$COMPUTE_SA" \
    --role="roles/dlp.user"
```

### Continuous Query Service Account

Create a dedicated service account for the continuous query:

```bash
# Create service account
gcloud iam service-accounts create bq-continuous-query \
    --project=$PROJECT_ID \
    --display-name="BigQuery Continuous Query Service Account"

CQ_SA="bq-continuous-query@${PROJECT_ID}.iam.gserviceaccount.com"

# Grant Pub/Sub publisher
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CQ_SA" \
    --role="roles/pubsub.publisher"

# Grant Pub/Sub viewer
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CQ_SA" \
    --role="roles/pubsub.viewer"

# Grant BigQuery data viewer
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CQ_SA" \
    --role="roles/bigquery.dataViewer"

# Grant user ability to use this service account
gcloud iam service-accounts add-iam-policy-binding $CQ_SA \
    --project=$PROJECT_ID \
    --member="user:YOUR_EMAIL@domain.com" \
    --role="roles/iam.serviceAccountUser"
```

## Step 4: Create Pub/Sub Resources

```bash
./scripts/create_pubsub.sh
```

This creates:
- `pii-logs-topic` - Main topic for CQ output
- `pii-logs-subscription` - Subscription for Dataflow to read (with exactly-once delivery enabled)
- `pii-logs-dlq` - Dead letter queue topic
- `pii-logs-dlq-subscription` - DLQ subscription

**Important:** The main subscription uses exactly-once delivery to prevent duplicate message processing. This is critical for data integrity.

## Step 5: Create BigQuery Reservation

**Important:** The reservation location must match your dataset location!

```bash
# Check your dataset location
bq show --format=prettyjson $PROJECT_ID:$SOURCE_DATASET | jq -r '.location'

# Update REGION in the script to match (e.g., "US" for multi-region)
./scripts/create_bq_reservation.sh
```

This creates:
- Enterprise edition reservation with 50 baseline slots
- Autoscaling up to 250 slots
- CONTINUOUS job type assignment

## Step 6: Create Cloud Storage for DLQ Archive

```bash
./scripts/create_dlq_storage.sh
```

This creates:
- GCS bucket: `gs://${PROJECT_ID}-dlq-archive`
- Pub/Sub subscription that writes DLQ messages to GCS
- Messages batched every 5 minutes or 10MB

## Step 7: Create Dataflow Temp Bucket

```bash
BUCKET="${PROJECT_ID}-dataflow-temp"
gsutil mb -p $PROJECT_ID -l us-central1 "gs://${BUCKET}"

# Grant Compute SA access
gsutil iam ch "serviceAccount:$COMPUTE_SA:roles/storage.objectAdmin" "gs://${BUCKET}"
```

## Step 8: Install Python Dependencies

```bash
uv sync
```

## Step 9: Deploy Dataflow Pipeline

```bash
uv run ./scripts/deploy_dataflow.sh
```

Wait for the job to reach "Running" state:
```bash
gcloud dataflow jobs list --project=$PROJECT_ID --region=us-central1 --limit=1
```

## Step 10: Start Continuous Query

```bash
./scripts/create_continuous_query.sh
```

The CQ runs continuously and captures new rows via the `APPENDS()` function.

---

## IAM Permissions Summary

| Service Account | Role | Purpose |
|----------------|------|---------|
| Compute Engine Default | `roles/dataflow.worker` | Dataflow worker operations |
| Compute Engine Default | `roles/pubsub.subscriber` | Read from Pub/Sub subscription |
| Compute Engine Default | `roles/pubsub.publisher` | Write to DLQ topic |
| Compute Engine Default | `roles/pubsub.viewer` | Query subscription configuration |
| Compute Engine Default | `roles/bigquery.dataEditor` | Write to destination table |
| Compute Engine Default | `roles/dlp.user` | Call DLP API for PII masking |
| CQ Service Account | `roles/pubsub.publisher` | Publish to Pub/Sub topic |
| CQ Service Account | `roles/pubsub.viewer` | Access topic metadata |
| CQ Service Account | `roles/bigquery.dataViewer` | Read source table |
| Pub/Sub Service Agent | `roles/storage.objectCreator` | Write DLQ to GCS |
| Pub/Sub Service Agent | `roles/storage.legacyBucketReader` | Access GCS bucket |

---

## Troubleshooting Guide

### Issue: "CONTINUOUS is not a supported object type"

**Cause:** BigQuery reservation not set up or wrong location.

**Fix:**
1. Verify reservation exists:
   ```bash
   bq ls --reservation --project_id=$PROJECT_ID --location=US
   ```
2. Verify assignment exists:
   ```bash
   bq ls --reservation_assignment --project_id=$PROJECT_ID --location=US
   ```
3. Ensure location matches dataset location

### Issue: "Unable to open file: gs://...pipeline.pb"

**Cause:** Compute Engine service account lacks GCS access.

**Fix:**
```bash
gsutil iam ch "serviceAccount:$COMPUTE_SA:roles/storage.objectAdmin" "gs://${PROJECT_ID}-dataflow-temp"
```

### Issue: "Missing permissions pubsub.subscriptions.consume"

**Cause:** Compute Engine SA lacks Pub/Sub subscriber role.

**Fix:**
```bash
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$COMPUTE_SA" \
    --role="roles/pubsub.subscriber"
```

### Issue: "Sensitive Data Protection (DLP) has not been used in project"

**Cause:** DLP API not enabled.

**Fix:**
```bash
gcloud services enable dlp.googleapis.com --project=$PROJECT_ID
```

### Issue: "access denied for permission serviceusage.services.use" (DLP)

**Cause:** Compute Engine SA lacks DLP user role.

**Fix:**
```bash
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$COMPUTE_SA" \
    --role="roles/dlp.user"
```

### Issue: "This field: request is not a record"

**Cause:** JSON field type mismatch between pipeline and BigQuery table.

**Fix:** Ensure destination table uses JSON type and pipeline converts dicts to JSON strings:
```sql
-- Check table schema
bq show --schema $PROJECT_ID:$SOURCE_DATASET.$BQ_OUTPUT_TABLE
```

### Issue: "constraints/compute.vmExternalIpAccess violated"

**Cause:** Org policy blocks external IPs on VMs.

**Fix:** Add `--no_use_public_ips` to Dataflow deployment and ensure Private Google Access is enabled on subnet:
```bash
gcloud compute networks subnets describe SUBNET_NAME \
    --project=$PROJECT_ID --region=REGION \
    --format="value(privateIpGoogleAccess)"
```

### Issue: "Network default is not accessible"

**Cause:** Default network doesn't exist or wrong network specified.

**Fix:** List available networks and update deploy script:
```bash
gcloud compute networks list --project=$PROJECT_ID
gcloud compute networks subnets list --project=$PROJECT_ID --filter="region:REGION"
```

### Issue: Continuous Query stops unexpectedly

**Cause:** CQ jobs can stop for various reasons (quota, errors, etc.)

**Fix:**
1. Check job status:
   ```bash
   bq ls --jobs --project_id=$PROJECT_ID --location=US --max_results=5
   ```
2. Restart CQ:
   ```bash
   ./scripts/create_continuous_query.sh
   ```

### Issue: Duplicate records in destination table

There are two common causes of duplicate records:

#### Cause 1: Multiple Continuous Queries Running
Each CQ publishes the same records to Pub/Sub, causing N×duplication where N is the number of running CQs.

**Diagnosis:**
```sql
-- Check for multiple running CQs (bq ls --jobs does NOT reliably show CQs!)
SELECT job_id, start_time
FROM `PROJECT_ID.region-US.INFORMATION_SCHEMA.JOBS`
WHERE continuous IS TRUE AND state = "RUNNING"
```

**Fix:**
1. Cancel all duplicate CQ jobs (keep only one):
   ```bash
   bq --project_id=$PROJECT_ID --location=US cancel JOB_ID
   ```
2. Purge old messages and truncate:
   ```bash
   gcloud pubsub subscriptions seek pii-logs-subscription \
       --project=$PROJECT_ID --time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
   bq query --use_legacy_sql=false "TRUNCATE TABLE \`$PROJECT_ID.$SOURCE_DATASET.$BQ_OUTPUT_TABLE\`"
   ```

#### Cause 2: Pub/Sub At-Least-Once Delivery (Root Cause)
By default, Pub/Sub uses at-least-once delivery, which can redeliver messages if acknowledgment is slow.

**Fix:** Enable exactly-once delivery on the subscription:
```bash
gcloud pubsub subscriptions update pii-logs-subscription \
    --project=$PROJECT_ID \
    --enable-exactly-once-delivery
```

**Note:** The `create_pubsub.sh` script already enables exactly-once delivery. This issue only occurs if the subscription was created manually without this flag.

**Prevention:** Before starting a new CQ, always check for existing running CQs using INFORMATION_SCHEMA:
```sql
SELECT
  start_time,
  job_id,
  user_email,
  query,
  state,
  reservation_id,
  continuous_query_info.output_watermark
FROM `PROJECT_ID.region-REGION.INFORMATION_SCHEMA.JOBS`
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
  AND continuous IS TRUE
  AND state = "RUNNING"
ORDER BY
  start_time DESC
```

### Issue: Messages in Pub/Sub but not reaching destination table

**Debug steps:**
1. Check Pub/Sub subscription has messages:
   ```bash
   gcloud pubsub subscriptions pull SUBSCRIPTION --project=$PROJECT_ID --limit=5
   ```
2. Check Dataflow job logs:
   ```bash
   JOB_ID=$(gcloud dataflow jobs list --project=$PROJECT_ID --region=REGION --limit=1 --format="value(id)")
   gcloud logging read "resource.labels.job_id=\"$JOB_ID\" AND severity>=WARNING" \
       --project=$PROJECT_ID --limit=10 --format="value(timestamp,jsonPayload.message)"
   ```
3. Check DLQ for failed records:
   ```bash
   gcloud pubsub subscriptions pull pii-logs-dlq-subscription --project=$PROJECT_ID --limit=5
   ```

---

## Monitoring Commands

### Check Pipeline Status
```bash
# Dataflow job status
gcloud dataflow jobs list --project=$PROJECT_ID --region=us-central1 --limit=1
```

**Important:** To check continuous query status, use INFORMATION_SCHEMA (the `bq ls --jobs` command does not reliably show continuous queries):
```sql
SELECT
  job_id,
  start_time,
  state,
  reservation_id
FROM `PROJECT_ID.region-US.INFORMATION_SCHEMA.JOBS`
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
  AND continuous IS TRUE
  AND state = "RUNNING"
ORDER BY start_time DESC
```

### Check Data Flow
```bash
# Source table count
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`$PROJECT_ID.$SOURCE_DATASET.$SOURCE_TABLE\`"

# Destination table count
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`$PROJECT_ID.$SOURCE_DATASET.$BQ_OUTPUT_TABLE\`"

# Pub/Sub backlog
gcloud pubsub subscriptions pull pii-logs-subscription --project=$PROJECT_ID --limit=1
```

### Check Errors
```bash
# Dataflow errors
JOB_ID=$(gcloud dataflow jobs list --project=$PROJECT_ID --region=us-central1 --limit=1 --format="value(id)")
gcloud logging read "resource.labels.job_id=\"$JOB_ID\" AND severity=ERROR" \
    --project=$PROJECT_ID --limit=10 --format="value(timestamp,jsonPayload.message)"

# DLQ messages
gcloud pubsub subscriptions pull pii-logs-dlq-subscription --project=$PROJECT_ID --limit=5

# DLQ files in GCS
gsutil ls "gs://${PROJECT_ID}-dlq-archive/dlq/"
```

---

## Testing the Pipeline

1. **Insert test records:**
   ```bash
   uv run python scripts/simulate_logs.py
   ```

2. **Wait for processing** (15-30 seconds)

3. **Verify masked data:**
   ```bash
   bq query --use_legacy_sql=false \
       "SELECT userIamPrincipal, _masked_at, _masking_status
        FROM \`$PROJECT_ID.$SOURCE_DATASET.$BQ_OUTPUT_TABLE\`
        LIMIT 5"
   ```

4. **Check PII is masked:**
   - `userIamPrincipal` should show `*******************` pattern
   - `_masking_status` should be `SUCCESS`

---

## Cleanup / Reset

### Truncate Tables (Keep Schema)
```bash
bq query --use_legacy_sql=false "TRUNCATE TABLE \`$PROJECT_ID.$SOURCE_DATASET.$SOURCE_TABLE\`"
bq query --use_legacy_sql=false "TRUNCATE TABLE \`$PROJECT_ID.$SOURCE_DATASET.$BQ_OUTPUT_TABLE\`"
```

### Purge Old Messages from Pub/Sub
If you need to discard old messages in the subscription (e.g., after fixing a bug or before a clean test):
```bash
# Seek subscription to current time - discards all old messages
gcloud pubsub subscriptions seek pii-logs-subscription \
    --project=$PROJECT_ID \
    --time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
```

### Cancel Dataflow Job
```bash
JOB_ID=$(gcloud dataflow jobs list --project=$PROJECT_ID --region=us-central1 --filter="state:Running" --limit=1 --format="value(id)")
gcloud dataflow jobs cancel $JOB_ID --project=$PROJECT_ID --region=us-central1
```

### Delete Resources (Full Cleanup)
```bash
# Delete Dataflow job
gcloud dataflow jobs cancel $JOB_ID --project=$PROJECT_ID --region=us-central1

# Delete Pub/Sub subscriptions
gcloud pubsub subscriptions delete pii-logs-subscription --project=$PROJECT_ID
gcloud pubsub subscriptions delete pii-logs-dlq-subscription --project=$PROJECT_ID
gcloud pubsub subscriptions delete pii-logs-dlq-gcs --project=$PROJECT_ID

# Delete Pub/Sub topics
gcloud pubsub topics delete pii-logs-topic --project=$PROJECT_ID
gcloud pubsub topics delete pii-logs-dlq --project=$PROJECT_ID

# Delete BigQuery reservation assignment and reservation
bq rm --reservation_assignment --project_id=$PROJECT_ID --location=US RESERVATION_NAME.ASSIGNMENT_ID
bq rm --reservation --project_id=$PROJECT_ID --location=US RESERVATION_NAME

# Delete GCS buckets
gsutil rm -r "gs://${PROJECT_ID}-dataflow-temp"
gsutil rm -r "gs://${PROJECT_ID}-dlq-archive"
```

---

## Configuration Reference

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PROJECT_ID` | Yes | - | GCP project ID |
| `SOURCE_DATASET` | Yes | - | BigQuery dataset containing source table |
| `SOURCE_TABLE` | Yes | - | Source table name for continuous query |
| `BQ_OUTPUT_TABLE` | Yes | - | Destination table for masked data |
| `REGION` | No | us-central1 | Dataflow region |
| `BQ_LOCATION` | No | US | BigQuery dataset location |
| `TOPIC_NAME` | No | pii-logs-topic | Pub/Sub topic name |
| `SUBSCRIPTION_NAME` | No | pii-logs-subscription | Pub/Sub subscription name |
| `DLQ_TOPIC_NAME` | No | pii-logs-dlq | Dead letter topic name |
| `BASELINE_SLOTS` | No | 50 | BQ reservation baseline slots |
| `AUTOSCALE_MAX_SLOTS` | No | 250 | BQ reservation max slots |

### Scripts

| Script | Purpose |
|--------|---------|
| `scripts/setup_all.sh` | **Master setup script** - runs all steps in order |
| `scripts/create_pubsub.sh` | Create Pub/Sub topics and subscriptions |
| `scripts/create_bq_reservation.sh` | Create BigQuery Enterprise reservation |
| `scripts/create_continuous_query.sh` | Start continuous query export to Pub/Sub |
| `scripts/create_dlq_storage.sh` | Set up GCS archival for dead letter queue |
| `scripts/deploy_dataflow.sh` | Deploy Dataflow streaming pipeline |
| `scripts/simulate_logs.py` | Generate test data to simulate log ingestion |

---

## Key Learnings & Best Practices

### Continuous Queries
- **Always use INFORMATION_SCHEMA** to check for running CQs - `bq ls --jobs` does not reliably show them
- **Never run multiple CQs** on the same source table simultaneously - each will publish duplicates
- CQs require **BigQuery Enterprise Edition reservation** with CONTINUOUS job type assignment
- Reservation location must match dataset location (e.g., both "US")

### Pub/Sub
- **Always enable exactly-once delivery** for the main subscription to prevent duplicate processing
- Use `gcloud pubsub subscriptions seek` to purge old messages when resetting the pipeline
- The 600-second ack deadline provides buffer for slow DLP processing

### Dataflow
- Use `--no_use_public_ips` when org policy blocks external IPs
- Specify network and subnetwork explicitly if default network doesn't exist
- The Streaming Engine handles backpressure from slow DLP calls

### Data Integrity
- The pipeline adds `_masked_at` and `_masking_status` metadata to each record
- Failed records go to the dead letter queue and are archived to Cloud Storage
- JSON fields must be converted to strings for BigQuery streaming inserts
