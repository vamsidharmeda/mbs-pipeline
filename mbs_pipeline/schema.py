import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)
from mbs_pipeline.enums import SchemaDataType


def load_schema_json(file_path: str) -> dict:
    with open(file_path, "r") as f:
        return json.load(f)


def get_spark_type(type_string: str):
    type_mapping = {
        SchemaDataType.STRING.value: StringType(),
        SchemaDataType.DOUBLE.value: DoubleType(),
        SchemaDataType.INTEGER.value: IntegerType(),
    }
    return type_mapping.get(type_string, StringType())


def build_schema(schema_json: dict) -> StructType:
    return StructType(
        [
            StructField(field_name, get_spark_type(field_info["type"]), True)
            for field_name, field_info in schema_json.items()
        ]
    )


def get_mortgage_data_schema(schema_file: str) -> StructType:
    schema_json = load_schema_json(schema_file)
    return build_schema(schema_json)
