from pyspark.sql import SparkSession, DataFrame
from mbs_pipeline.schema import get_mortgage_data_schema
from mbs_pipeline.enums import FileFormat


def load_mortgage_data(
    spark: SparkSession,
    input_path: str,
    schema_file: str,
    file_format: str = FileFormat.CSV.value,
    separator: str = "|",
) -> DataFrame:
    schema = get_mortgage_data_schema(schema_file)
    if file_format == FileFormat.CSV.value:
        return spark.read.csv(input_path, schema=schema, sep=separator)
    elif file_format == FileFormat.PARQUET.value:
        return spark.read.parquet(input_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
