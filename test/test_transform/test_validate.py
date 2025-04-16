import pytest
import yaml
from chispa import assert_df_equality
from pyspark.sql.types import StructType

from src.validate.expressions import Expressions
from src.validate.sources import Sources
from src.validate.transformations import Transformations, validate_join_conditions
from src.validate.validate_sql import generate_dummy_dataframe
from src.validate.yaml import Yaml


@pytest.fixture
def sources():
    return [
        {
            "source": "source_tbl_A",
            "alias": "TBL_A",
            "columns": ["c1", "c2", "c3", "c4"],
        },
        {
            "source": "source_tbl_B",
            "alias": "TBL_B",
            "columns": ["c1", "c5", "c7"],
        },
        {
            "source": "source_tbl_C",
            "alias": "TBL_C",
            "columns": ["c2c", "c6", "c8"],
        },
    ]


@pytest.fixture
def sources_joins():
    return yaml.safe_load(
        """
        -   alias: TBLA
            columns: ["col01", "col02", "col03"]
            source: table_a
        -   alias: TBLB
            columns: ["col01", "col02", "col09"]
            source: table_b
        -   alias: TBLC
            columns: ["col01", "col02", "col10"]
            source: table_c
    """,
    )


@pytest.mark.parametrize(
    ("sources", "transformations", "output", "expected_logging"),
    [
        (
            # Input `sources`` list
            [{"alias": "TBLA", "columns": ["a", "b"]}],
            # Input `joins`` list
            [
                {
                    "join": {
                        "left_source": "TBLA",
                        "right_source": "TBLB",
                        "condition": ["TBLA.b = TBLB.c"],
                    }
                }
            ],
            # Output of validation should be False
            False,
            # Expected logging from input above
            [
                "Problem with join",
                "No Right TBLB.",
                "Available alias: ['TBLA']",
            ],
        ),
        (
            # Input `sources`` list
            [{"alias": "TBLB", "columns": ["a", "b"]}],
            # Input `joins`` list
            [
                {
                    "join": {
                        "left_source": "TBLA",
                        "right_source": "TBLB",
                        "condition": ["TBLA.a = TBLB.b"],
                    }
                }
            ],
            # Output of validation should be False
            False,
            # Expected logging from input above
            [
                "Problem with join",
                "No Left TBLA.",
                "Available alias: ['TBLB']",
            ],
        ),
        (
            # Correct input `sources`` list
            [
                {"alias": "TBL_D", "columns": ["a", "b", "c"]},
                {"alias": "TBL_E", "columns": ["a", "d", "e"]},
            ],
            # Correct Input `joins`` list
            [
                {
                    "join": {
                        "left_source": "TBL_D",
                        "right_source": "TBL_E",
                        "condition": ["TBL_D.a = TBL_E.a"],
                        "how": "left",
                    }
                }
            ],
            # Output of validation should be True
            True,
            # Expected logging from input above
            [
                "Join condition expressions validated successfully",
                "Joins validated successfully",
                "Transformations validated successfully",
            ],
        ),
        (
            # Correct input `sources`` list
            [
                {"alias": "TBL_D", "columns": ["a", "b", "c"]},
                {"alias": "TBL_E", "columns": ["a", "d", "e"]},
            ],
            # Wrong join
            [
                {
                    "join": {
                        "left_source": "TBL_D",
                        "right_source": "TBL_E",
                        "condition": ["TBL_D.a = TBL_E.a"],
                        "how": "error_join",
                    }
                }
            ],
            # Output of validation should be False
            False,
            # Expected logging from input above
            [
                "Join condition expressions validated successfully",
                "Problem with join",
                "how option 'error_join' is not a valid option",
                "Possible values for 'how': ['inner', 'cross', 'outer', 'full', 'fullouter', 'full_outer', 'left', 'leftouter', 'left_outer', 'right', 'rightouter', 'right_outer', 'semi', 'leftsemi', 'left_semi', 'anti', 'leftanti', 'left_anti']",  # noqa: E501
            ],
        ),
        (
            # Correct input `sources`` list
            [
                {"alias": "TBL_D", "columns": ["a", "b", "c"]},
                {"alias": "TBL_E", "columns": ["a", "d", "e"]},
            ],
            # No valid transformation steps
            [
                {
                    "no_join": {
                        "left_source": "TBL_D",
                        "right_source": "TBL_E",
                        "condition": ["TBL_D.a = TBL_E.a"],
                        "how": "error_join",
                    }
                }
            ],
            # Output of validation should be False
            False,
            # Expected logging from input above
            [
                "Structure of transformation steps is incorrect",
                "Expected sections: ['join', 'add_variables', 'aggregation', 'pivot', 'union', 'filter']",  # noqa: E501
                "Received sections: ['no_join']",
            ],
        ),
        (
            # Correct input `sources`` list
            [
                {"alias": "TBL_D", "columns": ["a", "b", "c"]},
                {"alias": "TBL_E", "columns": ["a", "d", "e"]},
            ],
            # Valid variable step
            [
                {
                    "add_variables": {
                        "source": "TBL_D",
                        "column_mapping": {
                            "var_concat_cols": "TBL_D.a || TBL_E.e",
                        },
                    }
                }
            ],
            # Output of validation should be False
            True,
            # Expected logging from input above
            [
                "Variables validated successfully",
                "Transformations validated successfully",
            ],
        ),
        (
            # Correct input `sources`` list
            [
                {"alias": "TBL_D", "columns": ["a", "b"]},
                {"alias": "TBL_E", "columns": ["a", "d"]},
            ],
            # UnValid variable step
            [
                {
                    "add_variables": {
                        "source": "TBL_D",
                        "column_mapping": {
                            "var_concat_cols": "TBL_D.a2 || TBL_E.e2",
                        },
                    }
                }
            ],
            # Output of validation should be False
            False,
            # Expected logging from input above
            [
                "Problem with expression(s):",
                "TBL_D.a2 || TBL_E.e2: "
                "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter "
                "with name `TBL_D`.`a2` cannot be resolved. Did you mean one of the "
                "following? [`TBL_D`.`a`, `TBL_E`.`a`, `TBL_D`.`b`, `TBL_E`.`d`].",
            ],
        ),
        (
            # Correct input `sources`` list
            [
                {"alias": "TBL_D", "columns": ["a", "b"]},
                {"alias": "TBL_E", "columns": ["a", "d"]},
            ],
            # Empty transformation step
            None,
            # Output of validation should be True
            True,
            # Expected logging from input above
            [
                "No transformation steps included",
            ],
        ),
        (
            # Happy flow Union
            [
                {
                    "source": "source_tbl_A",
                    "alias": "TBL_A",
                    "columns": ["c1", "c2", "c3", "c4"],
                },
                {
                    "source": "source_tbl_B",
                    "alias": "TBL_B",
                    "columns": ["c1", "c5", "c7"],
                },
                {
                    "source": "source_tbl_C",
                    "alias": "TBL_C",
                    "columns": ["c2c", "c6", "c8"],
                },
            ],
            [
                {
                    "union": {
                        "source": "TBL_B",
                        "alias": "TABLE_B_C",
                        "column_mapping": {
                            "TBL_B": {"c1": "c1", "c5": "c5"},
                            "TBL_C": {"c1": "c2c", "c5": "c8"},
                        },
                    }
                }
            ],
            True,
            [
                "Union expressions validated successfully",
                "Transformations validated successfully",
            ],
        ),
        # No Existing source table Union
        (
            [
                {
                    "source": "source_tbl_A",
                    "alias": "TBL_A",
                    "columns": ["c1", "c2", "c3", "c4"],
                },
                {
                    "source": "source_tbl_B",
                    "alias": "TBL_B",
                    "columns": ["c1", "c5", "c7"],
                },
                {
                    "source": "source_tbl_C",
                    "alias": "TBL_C",
                    "columns": ["c2c", "c6", "c8"],
                },
            ],
            [
                {
                    "union": {
                        "source": "TBL_B",
                        "alias": "TABLE_B_C",
                        "column_mapping": {
                            "TBL_B": {"c1": "c1", "c5": "c5"},
                            "TBL_D": {"c1": "c2c", "c5": "c8"},
                        },
                    }
                }
            ],
            False,
            ["Source table(s) ['TBL_D'] in TABLE_B_C not loaded."],
        ),
        # No Existing column Union
        (
            [
                {
                    "source": "source_tbl_A",
                    "alias": "TBL_A",
                    "columns": ["c1", "c2", "c3", "c4"],
                },
                {
                    "source": "source_tbl_B",
                    "alias": "TBL_B",
                    "columns": ["c1", "c5", "c7"],
                },
                {
                    "source": "source_tbl_C",
                    "alias": "TBL_C",
                    "columns": ["c2c", "c6", "c8"],
                },
            ],
            [
                {
                    "union": {
                        "source": "TBL_B",
                        "alias": "TABLE_B_C",
                        "column_mapping": {
                            "TBL_B": {"c1": "c1", "c5": "c5"},
                            "TBL_C": {"c1": "c_not_existing", "c5": "c8"},
                        },
                    }
                }
            ],
            False,
            [
                "Problem with expression(s):",
                "c_not_existing: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `c_not_existing` cannot be resolved. Did you mean one of the following? [`c2c`, `c6`, `c8`].",  # noqa: E501
            ],
        ),
        # Wrong column mapping Union
        (
            [
                {
                    "source": "source_tbl_A",
                    "alias": "TBL_A",
                    "columns": ["c1", "c2", "c3", "c4"],
                },
                {
                    "source": "source_tbl_B",
                    "alias": "TBL_B",
                    "columns": ["c1", "c5", "c7"],
                },
                {
                    "source": "source_tbl_C",
                    "alias": "TBL_C",
                    "columns": ["c2c", "c6", "c8"],
                },
            ],
            [
                {
                    "union": {
                        "source": "TBL_B",
                        "alias": "TABLE_B_C",
                        "column_mapping": {
                            "TBL_B": {"c1": "c1", "c5": "c5"},
                            "TBL_C": {"c1_wrng": "c2c", "c5": "c8"},
                        },
                    }
                }
            ],
            False,
            [
                "Column mapping names are not identical: [['c1', 'c5'], ['c1_wrng', 'c5']]"  # noqa: E501
            ],
        ),
        # Happy flow filter
        (
            [
                {"alias": "TBL_D", "columns": ["a", "b", "c"]},
                {"alias": "TBL_E", "columns": ["a", "d", "e"]},
            ],
            [{"filter": {"conditions": ["TBL_D.a > 0", "TBL_E.d IS NOT NULL"]}}],
            True,
            ["Filter conditions validated successfully"],
        ),
        # Filter with invalid column reference
        (
            [
                {"alias": "TBL_D", "columns": ["a", "b", "c"]},
                {"alias": "TBL_E", "columns": ["a", "d", "e"]},
            ],
            [
                {
                    "filter": {
                        "conditions": ["TBL_D.x > 0"]  # x doesn't exist
                    }
                }
            ],
            False,
            [
                "Problem with expression(s):",
                "filter_condition_0: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `TBL_D`.`x` cannot be resolved. Did you mean one of the following? [`TBL_D`.`a`, `TBL_E`.`a`, `TBL_D`.`b`, `TBL_D`.`c`, `TBL_E`.`d`],",  # noqa: E501
            ],
        ),
        # Filter with computed variable
        (
            [
                {"alias": "TBL_D", "columns": ["a", "b", "c"]},
            ],
            [
                {
                    "add_variables": {
                        "source": "TBL_D",
                        "column_mapping": {"sum_ab": "TBL_D.a + TBL_D.b"},
                    }
                },
                {"filter": {"conditions": ["sum_ab > 10"]}},
            ],
            True,
            [
                "Variables validated successfully",
                "Filter conditions validated successfully",
                "Transformations validated successfully",
            ],
        ),
    ],
)
def test_validate_transformations(
    spark_session, sources, transformations, output, expected_logging, caplog
):
    logic = {"sources": sources, "transformations": transformations}
    assert Transformations(spark_session, logic).validate() == output
    assert caplog.messages == expected_logging


@pytest.mark.parametrize(
    ("sources", "validation_output", "expected_logging"),
    [
        # Happy flow
        (
            [
                {"source": "table_D", "alias": "TBL_D", "columns": ["a", "b", "c"]},
                {"source": "table_E", "alias": "TBL_E", "columns": ["a", "d", "e"]},
            ],
            True,
            "Sources validated successfully",
        ),
        # Happy with filter
        (
            [
                {
                    "source": "table_D",
                    "alias": "TBL_D",
                    "columns": ["a", "b", "c"],
                    "filter": "b<2",
                },
                {"source": "table_E", "alias": "TBL_E", "columns": ["a", "d", "e"]},
            ],
            True,
            "Sources validated successfully",
        ),
        # Unhappy with filter
        (
            [
                {
                    "source": "table_D",
                    "alias": "TBL_D",
                    "columns": ["a", "b", "c"],
                    "filter": "d<2",
                },
                {"source": "table_E", "alias": "TBL_E", "columns": ["a", "d", "e"]},
            ],
            False,
            "Problem with table/columns for source {'source': 'table_D', 'alias': 'TBL_D', 'columns': ['a', 'b', 'c'], 'filter': 'd<2'}",  # noqa: E501
        ),
        # Unhappy with some SQL injection
        (
            [
                {
                    "source": "table_D",
                    "alias": "TBL_D",
                    "columns": ["a", "b", "c"],
                    "filter": "d<2;DROP TABLE TBL_D",
                },
                {"source": "table_E", "alias": "TBL_E", "columns": ["a", "d", "e"]},
            ],
            False,
            "Incorrect syntax for source {'source': 'table_D', 'alias': 'TBL_D', 'columns': ['a', 'b', 'c'], 'filter': 'd<2;DROP TABLE TBL_D'}",  # noqa: E501
        ),
        # Unhappy with duplicate alias
        (
            [
                {"source": "table_E", "alias": "TBL_E", "columns": ["a", "b", "c"]},
                {"source": "table_D", "alias": "TBL_E", "columns": ["a", "d", "e"]},
            ],
            False,
            "Risk on ambiguous columns, duplicate of source with alias: ['TBL_E']",
        ),
        # Unhappy with multiple duplicate alias
        (
            [
                {"source": "table_E", "alias": "TBL_E", "columns": ["a", "b", "c"]},
                {"source": "table_D", "alias": "TBL_E", "columns": ["a", "d", "e"]},
                {"source": "table_E", "alias": "TBL_C", "columns": ["a", "b", "c"]},
                {"source": "table_D", "alias": "TBL_C", "columns": ["a", "d", "e"]},
            ],
            False,
            "Risk on ambiguous columns, duplicate of source with alias: ['TBL_C', 'TBL_E']",  # noqa: E501
        ),
    ],
)
def test_validate_sources(
    spark_session, sources, validation_output, expected_logging, caplog
):
    for s in sources:
        spark_session.createDataFrame(
            [(1, 2, 3), (2, 7, 9)], schema=s["columns"]
        ).createOrReplaceTempView(s["source"])
    logic = {"sources": sources}
    assert Sources(spark_session, logic).validate() == validation_output
    assert caplog.messages[0] == expected_logging


@pytest.mark.parametrize(
    ("sources", "expressions", "validation_output", "expected_logging"),
    [
        # Happy flow
        (
            [
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
                    "columns": ["c2c", "c6", "c8"],
                    "data": [(1, "C", "D")],
                },
            ],
            {"concat_letters": "concat(TBL_B.c5, TBL_B.c7, TBL_C.c6, TBL_C.c8)"},
            True,
            ["Target expressions validated successfully"],
        ),
        # Happy with null value
        (
            [
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
                    "columns": ["c2c", "c6", "c8"],
                    "data": [(1, "C", "D")],
                },
            ],
            {"Col1": "NULL"},
            True,
            ["Target expressions validated successfully"],
        ),
        # Happy with emptystring value
        (
            [
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
                    "columns": ["c2c", "c6", "c8"],
                    "data": [(1, "C", "D")],
                },
            ],
            {"Col1": "''"},
            True,
            ["Target expressions validated successfully"],
        ),
        # Unhappy with non-existent field
        (
            [
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
                    "columns": ["c2c", "c6", "c8"],
                    "data": [(1, "C", "D")],
                },
            ],
            {"concat_letters": "concat(TBL_B.c5, TBL_B.c7, TBL_C.c6, TBL_C.c9)"},
            False,
            [
                "Problem with expression(s):",
                "concat(TBL_B.c5, TBL_B.c7, TBL_C.c6, TBL_C.c9): [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `TBL_C`.`c9` cannot be resolved. Did you mean one of the following? [`TBL_C`.`c6`, `TBL_C`.`c8`, `TBL_A`.`c1`, `TBL_B`.`c1`, `TBL_A`.`c2`].",  # noqa: E501
            ],
        ),
        # Unhappy with missing column name
        (
            [
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
                    "columns": ["c2c", "c6", "c8"],
                    "data": [(1, "C", "D")],
                },
            ],
            {"": "'DummyConstant'"},
            False,
            ["Problem with expression(s):", "'DummyConstant': Missing column name"],
        ),
        # Unhappy with missing value
        (
            [
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
                    "columns": ["c2c", "c6", "c8"],
                    "data": [(1, "C", "D")],
                },
            ],
            {"Col1": ""},
            False,
            ["Problem with expression(s):", "Col1: No value/expression given"],
        ),
        # Unhappy with some SQL injection
        (
            [
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
                    "columns": ["c2c", "c6", "c8"],
                    "data": [(1, "C", "D")],
                },
            ],
            {"col_drop": "TBL_A.a1;DROP TABLE TBL_B"},
            False,
            [
                "Problem with expression(s):",
                "TBL_A.a1;DROP TABLE TBL_B: "
                "[PARSE_SYNTAX_ERROR] Syntax error at or near ';'.(line 1, pos 8)",
            ],
        ),
    ],
)
def test_validate_expressions(
    spark_session, sources, expressions, validation_output, expected_logging, caplog
):
    logic = {"sources": sources, "expressions": expressions}

    assert Expressions(spark_session, logic).validate() == validation_output
    assert caplog.messages[: len(expected_logging)] == expected_logging


@pytest.mark.parametrize(
    ("sources", "conditions", "validation_output", "expected_logging"),
    [
        # Happy flow simple columns
        (
            [
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
                    "columns": ["c2c", "c6", "c8"],
                    "data": [(1, "C", "D")],
                },
            ],
            ["TBL_A.c1 = TBL_B.c1"],
            True,
            "Join condition expressions validated successfully",
        ),
        # Happy flow expression columns
        (
            [
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
                    "columns": ["c2c", "c6", "c8"],
                    "data": [(1, "C", "D")],
                },
            ],
            ["TBL_A.c1 = concat(TBL_B.c1, TBL_B.c5)"],
            True,
            "Join condition expressions validated successfully",
        ),
        # Unhappy with non-existent field
        (
            [
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
                    "columns": ["c2c", "c6", "c8"],
                    "data": [(1, "C", "D")],
                },
            ],
            ["TBL_A.c1 = concat(TBL_B.c5, TBL_B.c7, TBL_C.c6, TBL_C.c9)"],
            False,
            "concat(TBL_B.c5, TBL_B.c7, TBL_C.c6, TBL_C.c9): [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `TBL_C`.`c9` cannot be resolved. Did you mean one of the following? [`TBL_C`.`c6`, `TBL_C`.`c8`, `TBL_A`.`c1`, `TBL_B`.`c1`, `TBL_A`.`c2`].",  # noqa: E501
        ),
        # Unhappy with some SQL injection
        (
            [
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
                    "columns": ["c2c", "c6", "c8"],
                    "data": [(1, "C", "D")],
                },
            ],
            ["TBL_B.c1 = TBL_A.c1;DROP TABLE TBL_B"],
            False,
            "TBL_A.c1;DROP TABLE TBL_B: "
            "[PARSE_SYNTAX_ERROR] Syntax error at or near ';'.(line 1, pos 9)",
        ),
    ],
)
def test_validate_join_conditions(
    spark_session, sources, conditions, validation_output, expected_logging, caplog
):
    assert (
        validate_join_conditions(
            spark=spark_session, sources=sources, conditions=conditions
        )
        == validation_output
    )
    assert caplog.messages[-1] == expected_logging


@pytest.mark.parametrize(
    ("business_logic", "validation_output", "first_log", "last_log"),
    [
        (
            {
                "target": "TARGET_A",
                "sources": [
                    {"alias": "TBLA", "columns": ["c1", "c2"], "source": "tbl_a"}
                ],
                "expressions": {"new_c1": "TBLA.c1"},
            },
            True,
            "YAML format validated successfully",
            "YAML format validated successfully",
        ),
        (
            {
                "targettt": "TARGET_A",
                "sources": [
                    {"alias": "TBLA", "columns": ["c1", "c2"], "source": "tbl_a"}
                ],
                "expressions": {"new_c1": "TBLA.c1"},
            },
            False,
            "Structure of the business logic is incorrect",
            "Received sections: ['targettt', 'sources', 'expressions']",
        ),
        (
            {},
            False,
            "Structure of the business logic is incorrect",
            "Received sections: []",
        ),
    ],
)
def test_yaml(business_logic, validation_output, first_log, last_log, caplog):
    assert Yaml(business_logic).validate() is validation_output
    assert caplog.messages[0] == first_log
    assert caplog.messages[-1] == last_log


@pytest.mark.parametrize(
    ("transformations", "validation_output", "expected_logging"),
    [
        (  # Unhappy flow, duplicate in right source
            """
        - join:
            left_source: TBLA
            right_source: TBLB
            condition: ["TBLA.col01 = TBLB.col01"]
            how: left
        - join:
            right_source: TBLB
            condition: ["TBLA.col02 = TBLB.col02"]
            how: left
        """,
            False,
            [
                "Risk on ambiguous columns, using multiple times the right_source(s) ['TBLB']",  # noqa: E501
            ],
        ),
        (  # Unhappy flow, duplicate in left source
            """
        - join:
            left_source: TBLA
            right_source: TBLB
            condition: ["TBLA.col01 = TBLB.col01"]
            how: left
        - join:
            right_source: TBLA
            condition: ["TBLA.col02 = TBLB.col02"]
            how: left
        """,
            False,
            [
                "Risk on ambiguous columns, using multiple times the right_source(s) ['TBLA']",  # noqa: E501
            ],
        ),
        (  # Happy flow, no duplicate in right sources, since left_source is reinitiated
            """
        - join:
            left_source: TBLA
            right_source: TBLB
            condition: ["TBLA.col01 = TBLB.col01"]
            how: left
        - join:
            left_source: TBLA
            right_source: TBLB
            condition: ["TBLA.col02 = TBLB.col02"]
            how: left
        """,
            True,
            [
                "Join condition expressions validated successfully",
                "Joins validated successfully",
                "Join condition expressions validated successfully",
                "Joins validated successfully",
                "Transformations validated successfully",
            ],
        ),
        (  # UnHappy flow 1, join condition not possible, non existing TABLE alias
            """
        - join:
            left_source: TBLA
            right_source: TBLB
            condition: ["TBLA.col01 = TBLB.col01"]
            how: left
        - join:
            left_source: TBLA
            right_source: TBLB
            condition: ["TBLA.col02 = TBLD.col02"]
            how: left
        """,
            False,
            [
                "Join condition expressions validated successfully",
                "Joins validated successfully",
                "Problem with expression(s):",
                "TBLD.col02: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `TBLD`.`col02` cannot be resolved. Did you mean one of the following? [`TBLA`.`col02`, `TBLB`.`col02`, `TBLA`.`col01`, `TBLB`.`col01`, `TBLA`.`col03`].",  # noqa: E501
            ],
        ),
        (  # UnHappy flow 2, join condition not possible, non existing column
            """
        - join:
            left_source: TBLA
            right_source: TBLB
            condition: ["TBLA.col01 = TBLB.col01"]
            how: left
        - join:
            right_source: TBLC
            condition: ["TBLA.col02 = TBLC.col09"]
            how: left
        """,
            False,
            [
                "Join condition expressions validated successfully",
                "Joins validated successfully",
                "Problem with expression(s):",
                "TBLC.col09: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `TBLC`.`col09` cannot be resolved. Did you mean one of the following? [`TBLC`.`col01`, `TBLC`.`col02`, `TBLB`.`col09`, `TBLC`.`col10`, `TBLA`.`col01`].",  # noqa: E501
            ],
        ),
        (  # Unhappy flow, duplicate in multiple potential ambiguous table aliases in right_sources  # noqa: E501
            """
        - join:
            left_source: TBLA
            right_source: TBLB
            condition: ["TBLA.col01 = TBLB.col01"]
            how: left
        - join:
            right_source: TBLA
            condition: ["TBLA.col02 = TBLB.col02"]
            how: left
        - join:
            right_source: TBLB
            condition: ["TBLA.col02= TBLB.col02"]
            how: left
        """,
            False,
            [
                "Risk on ambiguous columns, using multiple times the right_source(s) ['TBLA', 'TBLB']",  # noqa: E501
            ],
        ),
        (  # Unhappy flow, duplicate in multiple parallel joins (multiple left_sources)
            """
        - join:
            left_source: TBLA
            right_source: TBLB
            condition: ["TBLA.col01 = TBLB.col01"]
            how: left
        - join:
            right_source: TBLB
            condition: ["TBLA.col02 = TBLB.col02"]
            how: left
        - join:
            left_source: TBLA
            right_source: TBLC
            condition: ["TBLA.col01 = TBLC.col01"]
            how: left
        - join:
            right_source: TBLC
            condition: ["TBLA.col02 = TBLC.col02"]
            how: left
        """,
            False,
            [
                "Risk on ambiguous columns, using multiple times the right_source(s) ['TBLB']",  # noqa: E501
                "Risk on ambiguous columns, using multiple times the right_source(s) ['TBLC']",  # noqa: E501
            ],
        ),
    ],
)
def test_ambiguous_columns(
    spark_session,
    transformations,
    validation_output,
    expected_logging,
    caplog,
    sources_joins,
):
    """Test scenario of ambiguous columns."""
    business_logic = {
        "sources": sources_joins,
        "transformations": yaml.safe_load(transformations),
    }
    assert (
        Transformations(spark_session, business_logic).validate() == validation_output
    )

    assert caplog.messages == expected_logging


@pytest.mark.parametrize(
    ("scenario", "output", "expected_logging"),
    [
        (  # Join
            """
            sources:
                -   alias: TBLA
                    columns: ["col01", "col02", "col03"]
                    source: table_a

            transformations:
                - join:
                    right_source: TBLA
                    condition: ["TBLA.col01 = TBLA.col01"]

            """,
            False,
            "Initial source should be specified",
        ),
        (  # Unhappy: Add Variables
            """
            sources:
                -   alias: TBLA
                    columns: ["col01", "col02", "col03"]
                    source: table_a

            transformations:
                - add_variables:
                    column_mapping:
                        var01: cast(col03 as string)

            """,
            False,
            "Initial source should be specified",
        ),
        (  # Happy: Add Variables
            """
            sources:
                -   alias: TBLA
                    columns: ["col01", "col02", "col03"]
                    source: table_a

            transformations:
                - add_variables:
                    source: TBLA
                    column_mapping:
                        var01: cast(col03 as string)

            """,
            True,
            "Variables validated successfully",
        ),
        (
            # Aggregation
            """
            sources:
                - alias: TBLA
                  source: table_a

            transformations:
                - aggregation:
                    alias: TBLA_AGG
                    group: ["col02", "col03"]
                    column_mapping:
                        max_col01: max(col01)
            """,
            False,
            "Initial source should be specified",
        ),
    ],
)
def test_initial_source(spark_session, caplog, scenario, output, expected_logging):
    logic = yaml.safe_load(
        scenario,
    )
    assert Transformations(spark_session, logic).validate() is output
    assert caplog.messages[0] == expected_logging


def test_generate_dummy_data_non_existent(spark_session, caplog):
    logic = yaml.safe_load(
        """
        sources:
        - alias: TBLA
          source: table_does_not_exist""",
    )
    assert_df_equality(
        generate_dummy_dataframe(spark_session, logic["sources"]),
        spark_session.createDataFrame([], StructType([])),
    )

    assert caplog.messages == [
        "Failed to retrieve columns for source table_does_not_exist"
    ]
