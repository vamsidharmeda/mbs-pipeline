import os

# Environment variables
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
DATA_SOURCE = os.getenv("DATA_SOURCE", "FREDDIE_MAC")
SCHEMA = os.getenv("SCHEMA")
SCHEMA_MAPPER = os.getenv("SCHEMA_MAPPER")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

