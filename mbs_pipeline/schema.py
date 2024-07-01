import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)
from mbs_pipeline.enums import SchemaDataType
from typing import Dict, Any


def load_schema_json(file_path: str) -> Dict[str, Any]:
    """
    Load schema definition from a JSON file.

    Args:
        file_path (str): Path to the JSON schema file.

    Returns:
        Dict[str, Any]: Loaded schema as a dictionary.
    """
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Schema file not found: {file_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON format in schema file: {file_path}")


def get_spark_type(type_string: str) -> Any:
    """
    Map a schema data type string to a PySpark data type.

    Args:
        type_string (str): Schema data type string.

    Returns:
        DataType: Corresponding PySpark data type.
    """
    type_mapping = {
        SchemaDataType.STRING.value: StringType(),
        SchemaDataType.DOUBLE.value: DoubleType(),
        SchemaDataType.INTEGER.value: IntegerType(),
    }
    return type_mapping.get(type_string, StringType())


def build_schema(schema_json: Dict[str, Dict[str, str]]) -> StructType:
    """
    Build a PySpark StructType schema from a JSON schema definition.

    Args:
        schema_json (Dict[str, Dict[str, str]]): Schema definition in JSON format.

    Returns:
        StructType: PySpark StructType schema.
    """
    return StructType(
        [
            StructField(field_name, get_spark_type(field_info["type"]), True)
            for field_name, field_info in schema_json.items()
        ]
    )


def get_mortgage_data_schema(schema_file: str) -> StructType:
    """
    Get the PySpark schema for mortgage data from a schema file.

    Args:
        schema_file (str): Path to the schema file.

    Returns:
        StructType: PySpark StructType schema.
    """
    schema_json = load_schema_json(schema_file)
    return build_schema(schema_json)