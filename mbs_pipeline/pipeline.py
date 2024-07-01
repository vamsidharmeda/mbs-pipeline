#!/usr/bin/env python3
import asyncio
import logging
from typing import Dict, List
from pyspark.sql import SparkSession
from mbs_pipeline.scraper import FreddieMacScraper
from mbs_pipeline.transform import transform_mortgage_data
from mbs_pipeline.load import load_mortgage_data
from mbs_pipeline.writer import write_to_parquet, write_to_bigquery
from mbs_pipeline.enums import FileFormat, DataSource, ConfigKey, WriteMode
from mbs_pipeline.utils import get_config
from settings import (
    USERNAME,
    PASSWORD,
    DATA_SOURCE,
    GCS_BUCKET_NAME
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

CONFIG_PATH  = "mbs_pipeline/{partner}/pipeline_config.yaml"
async def run_scraper() -> List[str]:
    scraper = FreddieMacScraper(
        USERNAME, PASSWORD, DataSource[DATA_SOURCE].value, GCS_BUCKET_NAME
    )
    return await scraper.scrape()


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("MortgageDataProcessor")
        .getOrCreate()
    )


def process_data(spark: SparkSession, raw_path: str, config: Dict[str, str]) -> None:
    logging.info(f"Processing data from {raw_path}")
    df = load_mortgage_data(
        spark,
        raw_path,
        next(config[ConfigKey.SCHEMA.value] for config in config[ConfigKey.DATA_SOURCES.value] if config[ConfigKey.NAME.value] == DataSource[DATA_SOURCE].value),
        config.get(ConfigKey.FILE_FORMAT.value, FileFormat.CSV.value),
        config.get(ConfigKey.SEPARATOR.value, "|"),
    )
    df_transformed = transform_mortgage_data(
        df, next(config[ConfigKey.SCHEMA_MAPPER.value] for config in config[ConfigKey.DATA_SOURCES.value] if config[ConfigKey.NAME.value] == DataSource[DATA_SOURCE].value)
    )

    output_path = f"{config[ConfigKey.OUTPUT_BASE_PATH.value]}/{raw_path.split('/')[-1].replace('.txt', '')}"
    write_to_parquet(df_transformed, output_path)

    logging.info("Loading data to BigQuery")
    write_to_bigquery(
        df_transformed,
        config[ConfigKey.PROJECT_ID.value],
        config[ConfigKey.DATASET_ID.value],
        config[ConfigKey.TABLE_ID.value],
        config[ConfigKey.TEMP_BUCKET.value],
    )


def run_pipeline():
    loop = asyncio.get_event_loop()
    raw_gcs_paths = loop.run_until_complete(run_scraper())
    spark = create_spark_session()

    config = get_config(CONFIG_PATH.format(partner=DataSource[DATA_SOURCE].value))

    for raw_path in raw_gcs_paths:
        process_data(spark, raw_path, config)

    spark.stop()


if __name__ == "__main__":
    run_pipeline()
