from pyspark.sql import DataFrame


def write_to_parquet(df: DataFrame, output_path: str) -> None:
    df.write.partitionBy("reporting_year", "reporting_month").parquet(
        output_path, mode="overwrite"
    )


def write_to_bigquery(
    df: DataFrame, project_id: str, dataset_id: str, table_id: str, temp_bucket: str
) -> None:
    df.write.format("bigquery").option(
        "table", f"{project_id}:{dataset_id}.{table_id}"
    ).option("temporaryGcsBucket", temp_bucket).mode("append").save()
