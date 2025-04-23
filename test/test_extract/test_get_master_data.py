import pytest
from chispa import assert_df_equality
from pyspark.sql.types import StringType, StructField, StructType

from src.extract.master_data_sql import GetIntegratedData, parse_join_condition
from src.utils.alias_util import get_aliases_on_dataframe

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
    {
        "source": "source_tbl_C",
        "alias": "TBL_C",
        "columns": ["c1c", "c6", "c8"],
        # c1c colom with value 1 has two records,
        # so we expect two rows in final output
        "data": [(1, "C", "D"), (1, "E", "F")],
    },
    {
        "source": "source_tbl_C2",
        "alias": "TBL_C2",
        "columns": ["c2c", "c6", "c8"],
        "data": [(1, "C", "D")],
    },
    {
        "source": "source_tbl_D",
        "alias": "TBL_D_r",
        "columns": ["d1", "d2", "d3", "d4"],
        "data": [(1, 2, 3, 4)],
    },
    {
        "source": "source_tbl_D",
        "alias": "TBL_D_l",
        "columns": ["d1", "d2", "d3", "d4"],
        "data": [(1, 2, 3, 4)],
    },
    {
        "source": "source_tbl_D",
        "alias": "TBL_D",
        "columns": ["d1", "d2", "d3", "d4"],
        "data": [(1, 2, 3, 4)],
    },
]

JOINS = [
    {
        "right_source": "TBL_B",
        "condition": ["TBL_A.c1 = TBL_B.c1"],
        "how": "left",
    },
    {
        "right_source": "TBL_C",
        "condition": ["TBL_A.c1 = TBL_C.c1c"],
        "how": "left",
    },
    {
        "right_source": "TBL_C2",
        "condition": ["TBL_A.c1 = TBL_C2.c2c"],
        "how": "left",
    },
    {
        "right_source": "TBL_D_r",
        "condition": ["TBL_A.c1 = CASE WHEN TBL_D_r.d1 = 1 THEN 1 END"],
        "how": "left",
    },
    {
        "right_source": "TBL_D_l",
        "condition": ["CASE WHEN TBL_A.c1 = 1 THEN 1 END = TBL_D_l.d1 = 1"],
        "how": "left",
    },
    {
        "right_source": "TBL_D",
        "condition": [
            "CASE WHEN TBL_A.c1 = 1 THEN 1 END = CASE WHEN TBL_D.d1 = 1 THEN 1 END"
        ],
        "how": "left",
    },
]


@pytest.mark.parametrize(
    ("right_sources", "output_data"),
    [
        # All tables are joined from `JOINS` list, so 13 columns in total
        (
            ["TBL_B", "TBL_C", "TBL_C2"],
            [
                (1, 2, 3, 4, 1, "A", "B", 1, "E", "F", 1, "C", "D"),
                (1, 2, 3, 4, 1, "A", "B", 1, "C", "D", 1, "C", "D"),
            ],
        ),
        # Subset of tables is joined from `JOINS` list, so 10 columns in total
        (
            ["TBL_C", "TBL_C2"],
            [
                (1, 2, 3, 4, 1, "E", "F", 1, "C", "D"),
                (1, 2, 3, 4, 1, "C", "D", 1, "C", "D"),
            ],
        ),
        # One to one mapping, no transformations
        (
            ["TBL_A"],
            [
                (1, 2, 3, 4),
            ],
        ),
        # Checking CASE statements
        (
            ["TBL_D_r", "TBL_D_l", "TBL_D"],
            [
                (1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4),
            ],
        ),
    ],
)
def test_full_master_data_sql(spark_session, right_sources, output_data):
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
        "transformations": [
            {"join": j} for j in JOINS if j["right_source"] in right_sources
        ],
    }

    if len(business_logic_dict["transformations"]) == 0:
        business_logic_dict.pop("transformations", None)
    else:
        business_logic_dict["transformations"][0]["join"] = {
            "left_source": "TBL_A",
            **business_logic_dict["transformations"][0]["join"],
        }

    result = GetIntegratedData(
        spark_session,
        business_logic=business_logic_dict,
    ).get_integrated_data()

    expected_schema = [
        f"{s['alias']}.{col}"
        for s in SOURCES
        for col in s["columns"]
        if s["alias"] in ["TBL_A", *right_sources]
    ]
    expected_output = spark_session.createDataFrame(
        output_data, schema=expected_schema
    ).transform(get_aliases_on_dataframe, input_field_names=expected_schema)
    assert_df_equality(result, expected_output)


@pytest.mark.parametrize(
    ("condition_str", "match"),
    [
        # Three times is not allowed
        (
            "case when a=1 then 2 end = case when b=2 then 2 end = case when c=3 then 3 end",  # noqa: E501
            "Regex pattern split the condition in 3",
        )
    ],
)
def test_raise_parse_join_conditions(condition_str, match):
    with pytest.raises(
        ValueError,
        match=match,
    ):
        parse_join_condition(condition_str)


def test_union_data(spark_session, get_integrated_data_mock):
    # Union configuration
    unions = {
        "alias": "TABLE_B_C",
        "column_mapping": {
            "TABLE_B": {
                "col01": "cast(TABLE_B.col01 as string)",
                "col04": "TABLE_B.col04",
            },
            "TABLE_C": {
                "col01": "TABLE_C.col01",
                "col04": "TABLE_C.col05 / TABLE_C.col06",
            },
        },
    }
    expected_output = {
        # Input sources
        "TABLE_B": spark_session.createDataFrame(
            [(1, 5), (2, 6)], schema=["col01", "col04"]
        ),
        "TABLE_C": spark_session.createDataFrame(
            [("3", 7, 8), ("4", 9, 4)], schema=["col01", "col05", "col06"]
        ),
        # Additional union data
        "TABLE_B_C": spark_session.createDataFrame(
            [("1", 5.0), ("2", 6.0), ("3", 0.875), ("4", 2.25)],
            schema=["col01", "col04"],
        ),
    }
    # Select input sources
    input_sources = {
        k: v.alias(k) for k, v in expected_output.items() if k in ["TABLE_B", "TABLE_C"]
    }

    union_data = get_integrated_data_mock.union(
        input_sources["TABLE_B"], [input_sources["TABLE_C"]], **unions
    )
    assert_df_equality(union_data, expected_output["TABLE_B_C"])


def test_pivot_data(spark_session, get_integrated_data_mock):
    pivot_mapping = {
        "alias": "TBL_PIVOT",
        "group_cols": ["TBL_A.col01", "TBL_A.col02"],
        "pivot_col": "TBL_B.col04",
        "pivot_value_col": "TBL_A.col03",
        "column_mapping": {"X": "min", "Y": "min", "Z": "min"},
    }
    left_data = spark_session.createDataFrame(
        [("a", "b", "c"), ("a", "b", "d")], ["col01", "col02", "col03"]
    ).alias("TBL_A")
    right_data = spark_session.createDataFrame(
        [("a", "X"), ("a", "Y"), ("b", "Z")], ["col01", "col04"]
    ).alias("TBL_B")
    left_right = left_data.join(
        right_data, on=left_data.col01 == right_data.col01, how="left"
    )
    result = get_integrated_data_mock.pivot(left_right, **pivot_mapping)
    expected_output = spark_session.createDataFrame(
        [("a", "b", "c", "c", None)],
        schema=StructType(
            [
                StructField(c, StringType(), nullable=True)
                for c in ["col01", "col02", "X", "Y", "Z"]
            ]
        ),
    )
    assert_df_equality(result, expected_output)


def test_filter_transformation(spark_session):
    """Test filter transformation step in GetIntegratedData."""
    # Create test data
    source_data = {
        "source_tbl_A": [
            (1, "keep", 10),
            (2, "discard", 20),
            (3, "keep", 30),
        ],
    }
    # Create source dataframes and register as temp views
    spark_session.createDataFrame(
        source_data["source_tbl_A"], schema=["c1", "c2", "c3"]
    ).createOrReplaceTempView("source_tbl_A")
    # Define business logic with filter transformation
    business_logic_dict = {
        "sources": [
            {
                "source": "source_tbl_A",
                "alias": "TBL_A",
                "columns": ["c1", "c2", "c3"],
                "filter": "c2 = 'keep'",
            }
        ],
    }
    # Get integrated data with filter applied
    result = GetIntegratedData(
        spark_session,
        business_logic=business_logic_dict,
    ).get_integrated_data()
    # Check that only rows with c2='keep' are in the result
    expected_output = spark_session.createDataFrame(
        [(1, "keep", 10), (3, "keep", 30)], schema=["c1", "c2", "c3"]
    ).alias("TBL_A")
    assert_df_equality(result, expected_output, ignore_column_order=True)
