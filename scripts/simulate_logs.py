import os
import sys
import time
import uuid
import random
from datetime import datetime, timezone
from google.cloud import bigquery

# --- CONFIGURATION ---
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("SOURCE_DATASET")
TABLE_ID = os.environ.get("SOURCE_TABLE")

if not all([PROJECT_ID, DATASET_ID, TABLE_ID]):
    print("ERROR: Required environment variables not set.")
    print("Please set: PROJECT_ID, SOURCE_DATASET, SOURCE_TABLE")
    sys.exit(1)

client = bigquery.Client(project=PROJECT_ID)
table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

# --- MOCK DATA CONSTANTS ---
# Derived from Agentspace documentation [cite: 38-42, 53]
SERVICE_LABELS = ["AGENTSPACE", "NOTEBOOKLM_ENTERPRISE"]
METHOD_NAMES = [
    "SearchService.Search", 
    "AssistantService.Assist", 
    "AssistantService.StreamAssist",
    "ConversationalSearchService.AnswerQuery",
    "UserEventService.WriteUserEvent"
]
USERS = ["engineer@example.com", "analyst@example.com", "manager@company.org"]

def generate_fake_row():
    """Generates a single row of data matching the BigQuery schema ."""
    user_email = random.choice(USERS)
    service = random.choice(SERVICE_LABELS)
    
    return {
        "request": "{\"header\": \"mock_header\", \"body\": \"mock_body\"}", # [cite: 8]
        "response": "{\"status\": \"200\", \"content_length\": 1024}", # [cite: 9]
        "userIamPrincipal": f"user:{user_email}", # [cite: 10, 48]
        "timestamp": datetime.now(timezone.utc).isoformat(), # [cite: 11]
        "userQuery": "What is the status of the PII project?", # [cite: 12, 47]
        "serviceTextReply": "The project is currently in the ingestion phase.", # [cite: 13, 49]
        "serviceLabel": service, # [cite: 14, 53]
        "methodName": random.choice(METHOD_NAMES), # [cite: 15, 38-42]
        "serviceAttributionToken": str(uuid.uuid4()), # [cite: 16, 50]
        "serviceName": "discoveryengine.googleapis.com" # [cite: 17]
    }

def stream_data(row_count=10, delay=1):
    """Streams rows into BigQuery to simulate real-time ingestion."""
    print(f"Starting stream of {row_count} rows to {TABLE_ID}...")
    
    for i in range(row_count):
        row = generate_fake_row()
        errors = client.insert_rows_json(table_ref, [row])
        
        if errors == []:
            print(f"Successfully inserted row {i+1} for {row['userIamPrincipal']}")
        else:
            print(f"Errors occurred: {errors}")
            
        time.sleep(delay)

if __name__ == "__main__":
    # Adjust row_count and delay as needed for your testing
    stream_data(row_count=5, delay=2)
