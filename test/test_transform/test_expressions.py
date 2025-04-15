import pytest
from chispa import assert_df_equality
from pyspark.sql.types import IntegerType, NullType, StringType, StructField, StructType

from src.extract.master_data_sql import GetIntegratedData
from src.transform.transform_business_logic_sql import transform_business_logic_sql

SOURCES = [
    {
        "source": "source_tbl_A",
        "alias": "TBL_A",
        "columns": ["c1", "c2", "c3", "c4"],
        "data": [(1, 2, 3, 4)],
    },
    {
        "source": "source_tbl_B",
        "alias": "TBL_B",
        "columns": ["c1", "c5", "c7"],
        "data": [(1, "A", "B")],
    },
]

JOINS = [
    {
        "left_source": "TBL_A",
        "right_source": "TBL_B",
        "condition": ["TBL_A.c1 = TBL_B.c1"],
        "how": "left",
    },
]


@pytest.mark.parametrize(
    ("expressions", "output_data", "output_schema"),
    [
        # Simple function expression
        (
            {"Concatenated": "TBL_B.c5 || TBL_B.c7"},
            [["AB"]],
            StructType([StructField("Concatenated", StringType(), nullable=True)]),
        ),
        # Constant expression integer
        (
            {"Zero": "0"},
            [[0]],
            StructType([StructField("Zero", IntegerType(), nullable=False)]),
        ),
        # Constant expression string
        (
            {"Hello": '"Hi"'},
            [["Hi"]],
            StructType([StructField("Hello", StringType(), nullable=False)]),
        ),
        # Constant expression empty string
        (
            {"EmptyString": '""'},
            [[""]],
            StructType([StructField("EmptyString", StringType(), nullable=False)]),
        ),
        # Constant expression NULL
        (
            {"Nulled": "NULL"},
            [[None]],
            StructType([StructField("Nulled", NullType(), nullable=True)]),
        ),
    ],
)
def test_expressions(spark_session, expressions, output_data, output_schema):
    """Test `get_full_master_data_sql`.

    In this test, all sources are loaded in temporary view.
    The total number of columns of the output dataset should match

    The number of columns specified in `SOURCES` & joined according
    to the configuration in `JOINS`.
    """
    for s in SOURCES:
        spark_session.createDataFrame(
            s["data"], schema=s["columns"]
        ).createOrReplaceTempView(s["source"])

    business_logic_dict = {
        "sources": SOURCES,
        "transformations": [{"join": j} for j in JOINS],
        "expressions": expressions,
    }
    if len(business_logic_dict["transformations"]) == 0:
        business_logic_dict.pop("transformations", None)

    integrated_data = GetIntegratedData(
        spark_session,
        business_logic=business_logic_dict,
    ).get_integrated_data()

    result = transform_business_logic_sql(integrated_data, business_logic_dict)

    expected_output = spark_session.createDataFrame(output_data, schema=output_schema)
    assert_df_equality(result, expected_output, ignore_row_order=True)
