import pytest
from pyspark.sql import SparkSession

from src.validate.expressions import Expressions
from src.validate.sources import Sources


@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()


@pytest.mark.parametrize(
    ("sources", "expressions", "expected_valid", "expected_log"),
    [
        # Test: Missing "columns" should default to "*"
        (
            [
                {"source": "source_tbl_A", "alias": "TBL_A"},
                {
                    "source": "source_tbl_B",
                    "alias": "TBL_B",
                },  # No "columns" key -> should use "*"
            ],
            {"new_col": "TBL_A.c1"},  # "c1" exists in TBL_A
            True,
            "Target expressions validated successfully",
        ),
        # Test: Referencing non-existent column should fail
        (
            [
                {"source": "source_tbl_A", "alias": "TBL_A"},
                {"source": "source_tbl_B", "alias": "TBL_B"},
            ],
            {"new_col": "TBL_A.c99"},  # "c99" does not exist in TBL_A
            False,
            "A column or function parameter with name `TBL_A`.`c99` cannot be resolved.",  # noqa: E501
        ),
        # Test: Ensure filtering works when "filter" is included
        (
            [
                {
                    "source": "table_D",
                    "alias": "TBL_D",
                    "columns": ["c1", "c2"],
                    "filter": "c2 < 5",
                },
            ],
            {"new_col": "TBL_D.c1"},  # Should pass because "a" exists
            True,
            "Target expressions validated successfully",
        ),
        # Test: Expression validation should fail for non-existent columns
        (
            [
                {"source": "table_D", "alias": "TBL_D", "columns": ["c1", "c2"]},
            ],
            {"new_col": "TBL_D.x"},  # "x" does not exist in TBL_D
            False,
            "A column or function parameter with name `TBL_D`.`x` cannot be resolved.",
        ),
    ],
)
def test_all_columns(
    spark_session, sources, expressions, expected_valid, expected_log, caplog
):
    """Test handling of missing 'columns' and expression validation."""

    # Create dummy tables in Spark:
    dummy_data = {
        "source_tbl_A": [(1, 2, 3, 4)],
        "source_tbl_B": [(1, 5, 7)],
        "table_D": [(1, 2)],
    }
    dummy_schemas = {
        "source_tbl_A": ["c1", "c2", "c3", "c4"],
        "source_tbl_B": ["c1", "c5", "c7"],
        "table_D": ["c1", "c2"],
    }

    for table_name, data in dummy_data.items():
        df = spark_session.createDataFrame(data, schema=dummy_schemas[table_name])
        df.createOrReplaceTempView(table_name)

    # Test Sources class
    logic = {"sources": sources}
    sources_validator = Sources(spark_session, logic)
    assert sources_validator.validate() is True

    # Test Expressions class
    logic["expressions"] = expressions
    expr_validator = Expressions(spark_session, logic)
    assert expr_validator.validate() == expected_valid

    # Validate expected log messages
    assert expected_log in caplog.text
