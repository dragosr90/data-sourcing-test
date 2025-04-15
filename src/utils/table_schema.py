from pathlib import Path

from pyspark.sql.types import DataType, _parse_datatype_json_string

from src.config.constants import PROJECT_ROOT_DIRECTORY


def get_schema_from_file(uc_schema: str, table_name: str) -> DataType:
    """
    Reads JSON schema from PROJECT_ROOT/table_structures/{uc_schema}/{table_name}.json
    and converts it to a PySpark schema
    """
    with Path.open(
        PROJECT_ROOT_DIRECTORY / f"table_structures/{uc_schema}/{table_name}.json"
    ) as schema_json:
        return _parse_datatype_json_string(schema_json.read())
