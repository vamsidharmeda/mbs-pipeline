import csv

import pandas as pd
import yaml
from google.cloud import storage
from io import BytesIO
from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .getOrCreate()
    )

def get_config(file_path: str) -> dict:
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data

async def async_upload_to_gcs(bucket_name, blob_name, data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)


async def read_from_gcs(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_string()
    return pd.read_csv(BytesIO(content))
