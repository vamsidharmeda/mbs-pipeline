import json
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr


def load_transformations(file_path: str) -> dict:
    with open(file_path, "r") as f:
        return json.load(f)


def transform_mortgage_data(df: DataFrame, transformations_file: str) -> DataFrame:
    transformations = load_transformations(transformations_file)

    for column, transformation in transformations.items():
        df = df.withColumn(column, expr(transformation))

    return df
