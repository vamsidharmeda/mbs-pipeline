import csv
import pandas as pd
import yaml
from google.cloud import storage
from io import BytesIO
from pyspark.sql import SparkSession
from typing import Dict, Any

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and configure a Spark session.

    Args:
        app_name (str): Name of the Spark application.

    Returns:
        SparkSession: Configured Spark session.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def get_config(file_path: str) -> Dict[str, Any]:
    """
    Load configuration from a YAML file.

    Args:
        file_path (str): Path to the YAML configuration file.

    Returns:
        Dict[str, Any]: Configuration dictionary.
    """
    try:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    except yaml.YAMLError:
        raise ValueError(f"Invalid YAML format in configuration file: {file_path}")

async def async_upload_to_gcs(bucket_name: str, blob_name: str, data: str) -> None:
    """
    Asynchronously upload data to Google Cloud Storage.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob in the GCS bucket.
        data (str): Data to upload.

    Returns:
        None
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)

async def read_from_gcs(bucket_name: str, blob_name: str) -> pd.DataFrame:
    """
    Asynchronously read data from Google Cloud Storage and return as a pandas DataFrame.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob in the GCS bucket.

    Returns:
        pd.DataFrame: Data read from GCS as a pandas DataFrame.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_string()
    return pd.read_csv(BytesIO(content))