from enum import Enum


class FileFormat(Enum):
    CSV = "csv"
    PARQUET = "parquet"


class DataSource(Enum):
    FREDDIE_MAC = "freddie_mac"
    FANNIE_MAE = "fannie_mae"
    GINNIE_MAE = "ginnie_mae"


class ConfigKey(Enum):
    PROJECT_ID = "project_id"
    DATASET_ID = "dataset_id"
    TABLE_ID = "table_id"
    TEMP_BUCKET = "temp_bucket"
    SCHEMA_MAPPER = "schema_mapper"
    DATA_SOURCES = "data_sources"
    NAME = "name"
    SCHEMA = "schema"
    OUTPUT_BASE_PATH = "output_base_path"
    FILE_FORMAT = "file_format"
    SEPARATOR = "separator"


class SchemaDataType(Enum):
    STRING = "string"
    DOUBLE = "double"
    INTEGER = "integer"


class WriteMode(Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"
