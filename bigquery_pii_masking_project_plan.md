# BigQuery PII Masking Pipeline — Project Plan

## Architecture

```
Source BQ Table → Continuous Query → Pub/Sub → Dataflow (SDP masking) → Masked BQ Table
                                                    ↓
                                              Dead Letter Queue
```

Daily audit job compares source vs. masked table to catch any gaps.

---

## Work Assignments

### Engineer 1: Infrastructure & Ingestion

- Define masked data table schema (coordinate with team)
- Create masked data BigQuery table
- Create Pub/Sub topic + subscription
- Document Pub/Sub message format
- Set up BigQuery Continuous Query to export to Pub/Sub
- Create IAM service accounts and permissions for all services
- Write Python script to simulate streaming events for testing

### Engineer 2: Dataflow Pipeline

- Build Dataflow streaming template (Pub/Sub → SDP → BigQuery)
- Configure SDP inspect/deidentify templates (coordinate on PII types)
- Implement error handling + dead letter queue
- Configure autoscaling and worker settings
- Write unit tests
- Deploy streaming job

### Engineer 3: Backfill & Audit

- Build one-time backfill script (batch process historical data through SDP)
- Handle SDP quota limits during backfill
- Build daily audit job to find missing records (older than 45 min in source but not in masked table)
- Set up alerts in Cloud Monitoring for audit failures
- Create simple monitoring dashboard

---

## Sync Points

**By end of Day 3, we need to agree on:**

1. Masked table schema
2. Pub/Sub message format (Engineers 1 & 2 must align before parallel work)
3. Which PII types to detect and how to mask each (Engineers 2 & 3 share SDP templates)

**Integration checkpoints:**

- Engineer 1 + 2: Test CQ → Pub/Sub → Dataflow → BQ end-to-end (target: end of week 2)
- Engineer 2 + 3: Validate backfill uses same SDP config as streaming (target: week 3)
- All: Full pipeline test with audit catching intentional gaps (target: week 4)

---

## Open Questions (Need Answers Before Starting)

1. What's the current table size and daily insert rate? (affects backfill timeline and Dataflow sizing)
2. Which PII types do we need to mask? Any domain-specific patterns beyond standard (email, phone, SSN)?
3. What masking method per type—redaction, character masking, or pseudonymization?
4. Is there a budget ceiling for Dataflow workers and SDP API calls?
5. What region is the source data in? (Continuous Queries have regional limitations)
6. Should the 45-minute audit threshold be configurable?

---

## Rough Timeline

- **Weeks 1-2:** Build components in parallel
- **Week 3:** Integration testing
- **Week 4:** Production deployment + start backfill
- **Weeks 5-6:** Monitor backfill, tune as needed

---

## Quick Reference: IAM Roles Needed

| Service Account | Roles |
|-----------------|-------|
| CQ Publisher | `bigquery.dataViewer`, `pubsub.publisher` |
| Dataflow Processor | `dataflow.worker`, `pubsub.subscriber`, `bigquery.dataEditor` |
| SDP Invoker | `dlp.user` |
