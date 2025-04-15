import pytest

from src.transform.table_write_and_comment import (
    source_dict_to_string,
    table_summary,
    target_expression_comments,
    transformation_dict_to_string,
    write_to_table,
)


@pytest.fixture
def business_logic_dict():
    return {
        "target": "some_target_table",
        "sources": [
            {"source": "table_D", "alias": "TBL_D", "columns": ["a", "b", "c"]},
            {"source": "table_E", "alias": "TBL_E", "columns": ["a", "d", "e"]},
        ],
        "transformations": [
            {
                "join": {
                    "left_source": "TBL_D",
                    "right_source": "TBL_E",
                    "condition": ["TBL_D.a = TBL_E.e"],
                    "how": "left",
                }
            },
            {
                "add_variables": {
                    "column_mapping": {
                        "Var1": "case when TBL_D.a>100 then TBL_D.a-100 else TBL_D.a end"  # noqa: E501
                    }
                }
            },
        ],
        "expressions": {
            "TargetCol1": "TBL_D.a",
            "TargetCol2": "TBL_E.e",
            "TargetCol3": 'case when TBL_D.b = "999" then TBL_E.a end',
        },
        "filter_target": ["TargetCol1 > 100", 'TargetCol2 = "hello"'],
        "drop_duplicates": True,
    }


@pytest.fixture
def data_to_write(spark_session):
    return spark_session.createDataFrame([(1, 3, 4)], schema=["A", "B", "C"])


@pytest.fixture
def mock_spark(mocker):
    return mocker.patch("pyspark.sql.SparkSession")


@pytest.mark.parametrize(
    ("transformation_dict", "output_string"),
    [
        (  # One join condition
            {
                "join": {
                    "left_source": "TBL_D",
                    "right_source": "TBL_E",
                    "condition": ["TBL_D.a = TBL_E.b"],
                    "how": "left",
                },
            },
            "- Join (left):\nTBL_D.a = TBL_E.b",
        ),
        (  # Multiple join conditions
            {
                "join": {
                    "left_source": "TBL_D",
                    "right_source": "TBL_E",
                    "condition": ["TBL_D.a = TBL_E.b", "TBL_D.f||TBL_D.g = TBL_E.e"],
                    "how": "left",
                },
            },
            "- Join (left):\n"
            "  - TBL_D.a = TBL_E.b\n"
            "  - TBL_D.f||TBL_D.g = TBL_E.e",
        ),
        (  # Aggregation
            {
                "aggregation": {
                    "group": ["TBL_D.a"],
                    "column_mapping": {"count_TBL_E_b": "count(TBL_E.b)"},
                }
            },
            "- Aggregate:\n"
            "  - group by ['TBL_D.a']\n"
            "  - count_TBL_E_b = count(TBL_E.b)\n",
        ),
        (  # Pivot
            {
                "pivot": {
                    "group_cols": ["TBL_A.col01", "TBL_A.col02"],
                    "pivot_col": "TBL_B.col04",
                    "pivot_value_col": "TBL_A.col03",
                    "column_mapping": {"X": "min", "Y": "avg", "Z": "first"},
                }
            },
            "- Pivot:\n"
            "  - group by ['TBL_A.col01', 'TBL_A.col02']\n"
            "  - pivot column TBL_B.col04\n"
            "  - values from column TBL_A.col03\n"
            "  - using values:\n"
            "    - min(X)\n"
            "    - avg(Y)\n"
            "    - first(Z)\n",
        ),
        # Add Variables
        (
            {
                "add_variables": {
                    "column_mapping": {
                        "NewCol01": "TBLA.col1",
                        "NewCol02": "cast(TBLB.col2 as string)",
                        "NewCol03": "TBLA.col3 + TBLC.col03",
                    },
                }
            },
            "- Variable:\n"
            "  - NewCol01 = TBLA.col1\n"
            "  - NewCol02 = cast(TBLB.col2 as string)\n"
            "  - NewCol03 = TBLA.col3 + TBLC.col03\n",
        ),
        # Union
        (
            {
                "union": {
                    "alias": "TABLE_A_B",
                    "column_mapping": {
                        "TABLE_A": {"c01_ab": "c01a", "c02_ab": "c02a"},
                        "TABLE_B": {"c01_ab": "c01b", "c02_ab": "c02b"},
                    },
                }
            },
            "- Union:\n"
            "  - TABLE_A_B:\n"
            "    - c01a as c01_ab, c02a as c02_ab FROM TABLE_A\n"
            "    - c01b as c01_ab, c02b as c02_ab FROM TABLE_B",
        ),
    ],
)
def test_transformation_dict_to_string(transformation_dict, output_string):
    assert transformation_dict_to_string(tf=transformation_dict) == output_string


def test_raise_transformation_dict_to_string():
    """Test Raise of ValueError for `transformation_dict_to_string` method."""
    with pytest.raises(
        ValueError,
        match=r"Input `tf` should be a dict with one of the following keys: .*",
    ):
        transformation_dict_to_string(
            {
                # Spelling error
                "aggregationnn": {
                    "group": ["TBL_D.a"],
                    "column_mapping": {"TBL_E.b": "count"},
                }
            }
        )


@pytest.mark.parametrize(
    ("source_dict", "output_string"),
    [
        (
            {
                "source": "source_data_table",
                "alias": "TBL_D",
                "columns": ["a", "b", "c"],
            },
            "- TBL_D = source_data_table",
        ),
        (
            {
                "source": "source_data_table",
                "alias": "TBL_D",
                "columns": ["a", "b", "c"],
                "filter": "b<1",
            },
            "- TBL_D = source_data_table  \nFilter: b<1",
        ),
    ],
)
def test_source_dict_to_string(source_dict, output_string):
    assert source_dict_to_string(source_dict) == output_string


def test_table_summary(mock_spark, business_logic_dict):
    """Test table summary function"""
    table_summary(
        mock_spark, business_logic_dict, sources_title="## Another Source title\n"
    )
    # We only want SparkSession.sql to have been called once, so assert that
    assert mock_spark.sql.call_count == 1
    expected_sql_call = (
        'COMMENT ON TABLE some_target_table IS "## Another Source title\n'
        "- TBL_D = table_D\n"
        "- TBL_E = table_E\n\n"
        "#### Transformations\n"
        "- Join (left):\n"
        "TBL_D.a = TBL_E.e\n"
        "- Variable:\n"
        "  - Var1 = case when TBL_D.a>100 then TBL_D.a-100 else TBL_D.a end\n\n"
        "**Filter Target**\n"
        "- TargetCol1 > 100\n"
        '- TargetCol2 = \\"hello\\"\n\n'
        'Drop duplicates = True"'
    )
    mock_spark.sql.assert_called_with(expected_sql_call)


def test_target_expression_comments(mocker, mock_spark, business_logic_dict):
    """Test target expression comments."""
    target_expression_comments(mock_spark, business_logic_dict)
    assert mock_spark.sql.call_count == 3
    call_1 = mocker.call(
        'ALTER TABLE some_target_table ALTER COLUMN TargetCol1 COMMENT "TBL_D.a"'
    )
    call_2 = mocker.call(
        'ALTER TABLE some_target_table ALTER COLUMN TargetCol2 COMMENT "TBL_E.e"'
    )
    call_3 = mocker.call(
        "ALTER TABLE some_target_table ALTER COLUMN TargetCol3 COMMENT "
        '"case when TBL_D.b = \\"999\\" then TBL_E.a end"'
    )
    mock_spark.sql.assert_has_calls([call_1, call_2, call_3])


@pytest.mark.parametrize("tbl_name", ["TBL_A", "TBL_B"])
@pytest.mark.parametrize("mode", ["overwrite", "append"])
def test_write_to_table(mocker, data_to_write, tbl_name, mode):
    mock_writer = mocker.MagicMock()
    mocker.patch(
        "pyspark.sql.DataFrame.write",
        new_callable=mocker.PropertyMock,
        return_value=mock_writer,
    )
    write_to_table(tbl_name, data=data_to_write, mode=mode)
    mock_writer.mode.assert_called_with(mode)
    mock_writer.mode().saveAsTable.assert_called_with(tbl_name)
