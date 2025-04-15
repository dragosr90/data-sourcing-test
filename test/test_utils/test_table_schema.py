from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.utils.table_schema import get_schema_from_file


def test_get_schema():
    schema = StructType(
        [
            StructField("MainIdentifier", LongType(), nullable=True),
            StructField("MaxCol3", LongType(), nullable=True),
            StructField("MedCol12", DoubleType(), nullable=True),
            StructField("AvgCol09", DoubleType(), nullable=True),
            StructField("NullInCase", StringType(), nullable=False),
            StructField("StrInCase", StringType(), nullable=False),
            StructField("ConstEmptyString", StringType(), nullable=False),
            StructField("ConstInt", IntegerType(), nullable=False),
            StructField("ConstString", StringType(), nullable=False),
        ]
    )
    assert (
        get_schema_from_file(uc_schema="../test/data", table_name="TEST_SCHEMA")
        == schema
    )
