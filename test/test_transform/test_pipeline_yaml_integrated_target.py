from pathlib import Path

import pytest
from chispa import assert_df_equality
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    NullType,
    StringType,
    StructField,
    StructType,
)

from scripts.run_mapping import run_mapping
from src.config.constants import PROJECT_ROOT_DIRECTORY
from src.extract.master_data_sql import GetIntegratedData
from src.transform.transform_business_logic_sql import (
    transform_business_logic_sql,
)
from src.utils.logging_util import get_logger
from src.utils.parse_yaml import parse_yaml
from src.validate.run_all import validate_business_logic_mapping

logger = get_logger()


@pytest.fixture
def source_data():
    return {
        "table_a": {
            "data": [
                (1, 2, 3, 1, 5, 1, 7),
                (2, 3, 3, 1, 5, 2, 7),
                (3, 1, 3, 0, 5, 3, 7),
            ],
            "schema": ["col01", "col02", "col03", "col04", "col04b", "col05", "col05b"],
        },
        "table_b": {
            "data": [
                (1, "keep", 6),
                (2, "keep", 7),
                (3, "keep", 8),
                (1, "no_keep", 9),
            ],
            "schema": ["col01", "col09", "col10"],
        },
        "table_c": {
            "data": [
                (1, 10, 11),
                (2, 10, 11),
                (3, 10, 11),
            ],
            "schema": ["col01c", "col11", "col12"],
        },
    }


@pytest.mark.parametrize(
    ("parameters", "target_table_name"),
    [
        (None, "test_catalog.test_schema_{{ RUN_MONTH }}.test_target_table"),
        (
            {"RUN_MONTH": "20240801"},
            "test_catalog.test_schema_20240801.test_target_table",
        ),
    ],
)
def test_pipeline_yaml_integrated_target(
    spark_session, source_data, parameters, target_table_name
):
    """Test full pipeline of YAML, Integrated data and (filtered) target attributes.

    Check `test/data/TEST_YAML.yml` for:
    - `sources`
    - `transformations`
    - `expressions`
    - `filter_target`
    """
    business_logic = parse_yaml("../test/data/TEST_YAML.yml", parameters=parameters)

    for s in source_data:
        spark_session.createDataFrame(**source_data[s]).createOrReplaceTempView(s)

    integrated_data = GetIntegratedData(
        spark_session,
        business_logic=business_logic,
    ).get_integrated_data()

    result = integrated_data.transform(
        transform_business_logic_sql,
        business_logic=business_logic,
    )

    expected_output = spark_session.createDataFrame(
        [
            {
                "MainIdentifier": 2,
                "MaxCol3": 3,
                "MedCol12": 11.0,
                "AvgCol09": 7.0,
                "NullInCase": "NOTNULL",
                "StrInCase": "keeping",
                "ConstNull1": None,
                "ConstNull2": None,
                "ConstString": "Hello World-123_! :)",
                "ConstEmptyString": "",
                "ConstInt": 0,
                "FirstCaseCol": 2,
            },
            {
                "MainIdentifier": 3,
                "MaxCol3": None,
                "MedCol12": 11.0,
                "AvgCol09": 8.0,
                "NullInCase": "NOTNULL",
                "StrInCase": "keeping",
                "ConstNull1": None,
                "ConstNull2": None,
                "ConstString": "Hello World-123_! :)",
                "ConstEmptyString": "",
                "ConstInt": 0,
                "FirstCaseCol": 2,
            },
        ],
        schema=StructType(
            [
                StructField("MainIdentifier", LongType(), nullable=True),
                StructField("MaxCol3", LongType(), nullable=True),
                StructField("MedCol12", DoubleType(), nullable=True),
                StructField("AvgCol09", DoubleType(), nullable=True),
                StructField("NullInCase", StringType(), nullable=False),
                StructField("StrInCase", StringType(), nullable=False),
                StructField("ConstNull1", NullType(), nullable=True),
                StructField("ConstNull2", NullType(), nullable=True),
                StructField("ConstString", StringType(), nullable=False),
                StructField("ConstEmptyString", StringType(), nullable=False),
                StructField("ConstInt", IntegerType(), nullable=False),
                StructField("FirstCaseCol", IntegerType(), nullable=True),
            ]
        ),
    )
    assert (
        validate_business_logic_mapping(
            spark=spark_session, business_logic=business_logic
        )
        is True
    )
    assert_df_equality(result, expected_output, ignore_row_order=True)
    assert business_logic["target"] == target_table_name


def test_pipeline_no_transformation(spark_session, source_data, caplog):
    """Test full pipeline without transformation step."""

    business_logic = {
        "target": "test_schema_202501.test_target_table",
        "sources": [
            {
                "alias": "TBLA",
                "columns": ["col01", "col02", "col03"],
                "source": "table_a",
            }
        ],
        "expressions": {"MainIdentifier": "TBLA.col01", "MaxCol3": "TBLA.col03"},
    }

    spark_session.createDataFrame(**source_data["table_a"]).createOrReplaceTempView(
        "table_a"
    )

    integrated_data = GetIntegratedData(
        spark_session,
        business_logic=business_logic,
    ).get_integrated_data()

    result = integrated_data.transform(
        transform_business_logic_sql,
        business_logic=business_logic,
    )

    expected_output = spark_session.createDataFrame(
        [(1, 3), (2, 3), (3, 3)],
        schema=StructType(
            [
                StructField("MainIdentifier", LongType(), nullable=True),
                StructField("MaxCol3", LongType(), nullable=True),
            ]
        ),
    )
    assert (
        validate_business_logic_mapping(
            spark=spark_session, business_logic=business_logic
        )
        is True
    )
    assert_df_equality(result, expected_output, ignore_row_order=True)
    assert business_logic["target"] == "test_schema_202501.test_target_table"
    assert caplog.messages == [
        "YAML format validated successfully",
        "Sources validated successfully",
        "No transformation steps included",
        "Target expressions validated successfully",
    ]


def test_pipeline_yaml_pivot(spark_session):
    business_logic = parse_yaml("../test/data/TEST_YAML_PIVOT.yml")
    source_dict = {
        "table_a": {
            "data": [("a", "b", 2), ("a", "b", 4)],
            "schema": ["col01", "col02", "col03"],
        },
        "table_b": {
            "data": [("a", "X"), ("a", "Y"), ("b", "Z")],
            "schema": ["col01", "col04"],
        },
        "table_c": {
            "data": [(2, 1.3, 1.8), (2, 2.6, 3.5)],
            "schema": ["X", "col05", "col06"],
        },
    }
    for source_name, source_data in source_dict.items():
        spark_session.createDataFrame(**source_data).createOrReplaceTempView(
            source_name
        )

    assert (
        validate_business_logic_mapping(
            spark=spark_session, business_logic=business_logic
        )
        is True
    )

    integrated_data = GetIntegratedData(
        spark_session,
        business_logic=business_logic,
    ).get_integrated_data()

    result = integrated_data.transform(
        transform_business_logic_sql,
        business_logic=business_logic,
    )

    expected_schema = StructType(
        [
            StructField("ID1", StringType(), nullable=True),
            StructField("ID2", StringType(), nullable=True),
            StructField("X", LongType(), nullable=True),
            StructField("Y", LongType(), nullable=True),
            StructField("Z", LongType(), nullable=True),
            StructField("V5", DoubleType(), nullable=True),
            StructField("V6", DoubleType(), nullable=True),
        ]
    )

    expected_output = spark_session.createDataFrame(
        [("a", "b", 2, 4, None, 1.3, 1.8), ("a", "b", 2, 4, None, 2.6, 3.5)],
        expected_schema,
    )
    assert_df_equality(result, expected_output, ignore_row_order=True)


def test_run_mapping(spark_session):
    """Test full pipeline from run_mapping script"""

    source_dict = {
        "table_a": {
            "data": [("a", "b", 2), ("a", "b", 4)],
            "schema": ["col01", "col02", "col03"],
        },
        "table_b": {
            "data": [("a", "X"), ("a", "Y"), ("b", "Z")],
            "schema": ["col01", "col04"],
        },
        "table_c": {
            "data": [(2, 1.3, 1.8), (2, 2.6, 3.5)],
            "schema": ["X", "col05", "col06"],
        },
    }
    for source_name, source_data in source_dict.items():
        spark_session.createDataFrame(**source_data).createOrReplaceTempView(
            source_name
        )

    run_mapping(
        spark_session,
        stage="test/data",
        target_mapping="TEST_YAML_PIVOT.yml",
        run_month="",
        local=True,
    )

    result = spark_session.read.table("test_pivot_table")

    expected_schema = StructType(
        [
            StructField("ID1", StringType(), nullable=True),
            StructField("ID2", StringType(), nullable=True),
            StructField("X", LongType(), nullable=True),
            StructField("Y", LongType(), nullable=True),
            StructField("Z", LongType(), nullable=True),
            StructField("V5", DoubleType(), nullable=True),
            StructField("V6", DoubleType(), nullable=True),
        ]
    )

    expected_output = spark_session.createDataFrame(
        [("a", "b", 2, 4, None, 1.3, 1.8), ("a", "b", 2, 4, None, 2.6, 3.5)],
        expected_schema,
    )

    assert_df_equality(
        result, expected_output, ignore_row_order=True, ignore_metadata=True
    )

    for file in Path.iterdir(
        PROJECT_ROOT_DIRECTORY / "spark-warehouse/test_pivot_table"
    ):
        Path.unlink(file)
    Path.rmdir(PROJECT_ROOT_DIRECTORY / "spark-warehouse/test_pivot_table")
