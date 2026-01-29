"""
Dataflow Streaming Pipeline for PII Masking

Reads messages from Pub/Sub (originating from BigQuery Continuous Query),
masks PII using Sensitive Data Protection (DLP), and writes to BigQuery.
Failed records are sent to a dead letter queue.
"""

import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub
from apache_beam.transforms.window import FixedWindows
from google.cloud import dlp_v2


class DLPMaskingConfig:
    """Configuration for DLP masking operations."""

    def __init__(self, project_id, deidentify_template=None, inspect_template=None):
        self.project_id = project_id
        self.deidentify_template = deidentify_template
        self.inspect_template = inspect_template


class MaskPIIFn(beam.DoFn):
    """DoFn that masks PII in specified fields using Cloud DLP."""

    # Fields that contain user-generated content requiring PII masking
    FIELDS_TO_MASK = ["userIamPrincipal"]

    def __init__(self, project_id, deidentify_template=None, inspect_template=None):
        self.project_id = project_id
        self.deidentify_template = deidentify_template
        self.inspect_template = inspect_template
        self.dlp_client = None

    def setup(self):
        """Initialize DLP client once per worker."""
        self.dlp_client = dlp_v2.DlpServiceClient()

    def _mask_text(self, text):
        """Mask PII in a single text field using DLP API."""
        if not text or not isinstance(text, str):
            return text

        parent = f"projects/{self.project_id}/locations/global"

        item = {"value": text}

        # Build the deidentify request
        request_params = {
            "parent": parent,
            "item": item,
        }

        # Use templates if provided, otherwise use default config
        if self.deidentify_template:
            request_params["deidentify_template_name"] = self.deidentify_template
        else:
            # Default deidentify config: mask with asterisks
            request_params["deidentify_config"] = {
                "info_type_transformations": {
                    "transformations": [
                        {
                            "primitive_transformation": {
                                "character_mask_config": {
                                    "masking_character": "*",
                                    "number_to_mask": 0,  # Mask all characters
                                }
                            }
                        }
                    ]
                }
            }

        if self.inspect_template:
            request_params["inspect_template_name"] = self.inspect_template
        else:
            # Default inspect config: common PII types
            request_params["inspect_config"] = {
                "info_types": [
                    {"name": "EMAIL_ADDRESS"},
                    {"name": "PHONE_NUMBER"},
                    {"name": "US_SOCIAL_SECURITY_NUMBER"},
                    {"name": "CREDIT_CARD_NUMBER"},
                    {"name": "PERSON_NAME"},
                    {"name": "STREET_ADDRESS"},
                    {"name": "DATE_OF_BIRTH"},
                    {"name": "IP_ADDRESS"},
                ]
            }

        response = self.dlp_client.deidentify_content(request=request_params)
        return response.item.value

    def process(self, element):
        """Process a single record, masking PII in relevant fields."""
        try:
            record = json.loads(element.decode("utf-8")) if isinstance(element, bytes) else element

            # Mask PII in specified fields
            for field in self.FIELDS_TO_MASK:
                if field in record and record[field]:
                    record[field] = self._mask_text(record[field])

            # Add processing metadata
            record["_masked_at"] = datetime.now(timezone.utc).isoformat()
            record["_masking_status"] = "SUCCESS"

            yield beam.pvalue.TaggedOutput("masked", record)

        except Exception as e:
            logging.error(f"Error masking record: {e}")
            # Send to dead letter queue
            error_record = {
                "original_data": element.decode("utf-8") if isinstance(element, bytes) else str(element),
                "error_message": str(e),
                "error_type": "MASKING_ERROR",
                "error_timestamp": datetime.now(timezone.utc).isoformat(),
            }
            yield beam.pvalue.TaggedOutput("dead_letter", error_record)


class FormatForBigQuery(beam.DoFn):
    """Format masked records for BigQuery insertion with proper JSON handling."""

    # Fields that should be JSON type in BigQuery
    JSON_FIELDS = ["request", "response"]

    def process(self, element):
        # Ensure all expected fields are present
        expected_fields = [
            "request", "response", "userIamPrincipal", "timestamp",
            "userQuery", "serviceTextReply", "serviceLabel",
            "methodName", "serviceAttributionToken", "serviceName",
            "_masked_at", "_masking_status"
        ]

        for field in expected_fields:
            if field not in element:
                element[field] = None

        # For JSON fields in BigQuery, streaming inserts need JSON strings
        # Convert dicts to JSON strings for proper insertion
        for field in self.JSON_FIELDS:
            if element.get(field) is not None:
                value = element[field]
                if isinstance(value, dict):
                    # Convert dict to JSON string for BQ JSON column
                    element[field] = json.dumps(value)
                elif isinstance(value, str):
                    # Validate it's valid JSON, keep as string
                    try:
                        json.loads(value)
                    except json.JSONDecodeError:
                        # Wrap invalid JSON in an object
                        element[field] = json.dumps({"raw_value": value})

        yield element


class HandleBigQueryErrors(beam.DoFn):
    """Handle BigQuery write errors and route to dead letter queue."""

    def process(self, element):
        # element format from failed_rows_with_errors: (table_id, row_dict, error_list)
        try:
            if hasattr(element, 'row'):
                # Named tuple format
                row = element.row
                errors = element.errors if hasattr(element, 'errors') else str(element)
            elif isinstance(element, (list, tuple)) and len(element) >= 2:
                # Tuple format - could be (table, row, errors) or (row, errors)
                if len(element) == 3:
                    _, row, errors = element
                else:
                    row, errors = element[0], element[1]
            else:
                row = element
                errors = "Unknown error format"

            error_record = {
                "original_data": json.dumps(row) if isinstance(row, dict) else str(row),
                "error_message": str(errors),
                "error_type": "BIGQUERY_WRITE_ERROR",
                "error_timestamp": datetime.now(timezone.utc).isoformat(),
            }
            yield json.dumps(error_record).encode("utf-8")
        except Exception as e:
            # Fallback error handling
            error_record = {
                "original_data": str(element),
                "error_message": f"Error processing failed row: {e}",
                "error_type": "BIGQUERY_WRITE_ERROR",
                "error_timestamp": datetime.now(timezone.utc).isoformat(),
            }
            yield json.dumps(error_record).encode("utf-8")


def run(argv=None):
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(description="PII Masking Dataflow Pipeline")

    # Required arguments
    parser.add_argument(
        "--input_subscription",
        required=True,
        help="Pub/Sub subscription to read from (format: projects/PROJECT/subscriptions/SUB)"
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="BigQuery output table (format: PROJECT:DATASET.TABLE)"
    )
    parser.add_argument(
        "--dead_letter_topic",
        required=True,
        help="Pub/Sub topic for dead letter queue (format: projects/PROJECT/topics/TOPIC)"
    )
    parser.add_argument(
        "--dlp_project",
        required=True,
        help="GCP project ID for DLP API calls"
    )

    # Optional arguments
    parser.add_argument(
        "--deidentify_template",
        default=None,
        help="DLP deidentify template name (optional)"
    )
    parser.add_argument(
        "--inspect_template",
        default=None,
        help="DLP inspect template name (optional)"
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    # Define BigQuery schema for masked table
    table_schema = {
        "fields": [
            {"name": "request", "type": "JSON", "mode": "NULLABLE"},
            {"name": "response", "type": "JSON", "mode": "NULLABLE"},
            {"name": "userIamPrincipal", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "userQuery", "type": "STRING", "mode": "NULLABLE"},
            {"name": "serviceTextReply", "type": "STRING", "mode": "NULLABLE"},
            {"name": "serviceLabel", "type": "STRING", "mode": "NULLABLE"},
            {"name": "methodName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "serviceAttributionToken", "type": "STRING", "mode": "NULLABLE"},
            {"name": "serviceName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "_masked_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_masking_status", "type": "STRING", "mode": "NULLABLE"},
        ]
    }

    # Create pipeline (don't use 'with' context manager for streaming jobs
    # as it waits for completion - we want to submit and return immediately)
    pipeline = beam.Pipeline(options=pipeline_options)

    # Read from Pub/Sub
    messages = pipeline | "ReadFromPubSub" >> ReadFromPubSub(
        subscription=known_args.input_subscription
    )

    # Mask PII with tagged outputs for success/failure
    masked_results = messages | "MaskPII" >> beam.ParDo(
        MaskPIIFn(
            project_id=known_args.dlp_project,
            deidentify_template=known_args.deidentify_template,
            inspect_template=known_args.inspect_template,
        )
    ).with_outputs("masked", "dead_letter")

    # Format and write successfully masked records to BigQuery
    formatted = (
        masked_results.masked
        | "FormatForBigQuery" >> beam.ParDo(FormatForBigQuery())
    )

    # Write to BigQuery with error handling
    bq_result = (
        formatted
        | "WriteToBigQuery" >> WriteToBigQuery(
            table=known_args.output_table,
            schema=table_schema,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR',
            method='STREAMING_INSERTS',
        )
    )

    # Handle BigQuery write failures - route to dead letter queue
    (
        bq_result.failed_rows_with_errors
        | "HandleBQErrors" >> beam.ParDo(HandleBigQueryErrors())
        | "WriteBQErrorsToDLQ" >> WriteToPubSub(topic=known_args.dead_letter_topic)
    )

    # Write masking failures to dead letter queue
    (
        masked_results.dead_letter
        | "FormatDeadLetter" >> beam.Map(lambda x: json.dumps(x).encode("utf-8"))
        | "WriteToDeadLetter" >> WriteToPubSub(topic=known_args.dead_letter_topic)
    )

    # Submit the pipeline to Dataflow and return immediately
    # (don't wait for streaming job to complete)
    result = pipeline.run()
    logging.info(f"Pipeline submitted. Job ID: {result.job_id()}")
    logging.info("Streaming job running on Dataflow. This process will now exit.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
