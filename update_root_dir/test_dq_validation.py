from pathlib import Path

import pytest

from abnamro_bsrc_etl.config.constants import MAPPING_ROOT_DIR
from abnamro_bsrc_etl.dq.dq_validation import DQValidation


@pytest.fixture
def source_data():
    return {
        "main_table": {
            "data": [
                (2, 2, None, "A"),
                (2, 1, 3, "B"),
                (3, 3, 3, None),
            ],
            "schema": ["PK1", "PK2", "PK3", "Type"],
        },
        "reference": {
            "data": [
                ("A", "A"),
                ("B", None),
                ("C", "C"),
                ("D", "D"),
            ],
            "schema": ["Lookup1", "Lookup2"],
        },
    }


@pytest.fixture
def main_data(spark_session, source_data, request):
    return (
        request.param,
        spark_session.createDataFrame(
            **source_data["main_table"]
        ).createOrReplaceTempView(request.param),
    )


@pytest.fixture
def reference_data(spark_session, source_data):
    return spark_session.createDataFrame(
        **source_data["reference"]
    ).createOrReplaceTempView("reference")


@pytest.mark.parametrize(
    ("main_data", "validation_output", "expected_logging"),
    [
        ("dq_test_happy", True, "Checks completed successfully"),
        (
            "dq_test_happy_col_num",
            True,
            "Checks completed successfully",
        ),
        ("dq_test_no_checks", None, "No checks done"),
        (
            "dq_test_unhappy_col_num",
            False,
            "Checks completed - DQ issues found",
        ),
        (
            "dq_test_unhappy_pk_dup",
            False,
            "Checks completed - DQ issues found",
        ),
        (
            "dq_test_unhappy_pk_null",
            False,
            "Checks completed - DQ issues found",
        ),
        (
            "dq_test_unhappy_not_null",
            False,
            "Checks completed - DQ issues found",
        ),
        (
            "dq_test_unhappy_num_cols",
            False,
            "Checks completed - DQ issues found",
        ),
        (
            "dq_test_unhappy_type_cols",
            False,
            "Checks completed - DQ issues found",
        ),
        (
            "dq_test_unhappy_ref",
            False,
            "Checks completed - DQ issues found",
        ),
        (
            "dq_test_unhappy_ref_filter",
            False,
            "Checks completed - DQ issues found",
        ),
        (
            "dq_test_unhappy_unique",
            False,
            "Checks completed - DQ issues found",
        ),
    ],
    indirect=["main_data"],
)
def test_dq_validation(
    spark_session,
    validation_output,
    reference_data,
    expected_logging,
    main_data,
    caplog,
):
    """Test full DQ validation for a given table"""

    table_name = main_data[0]

    dq_validation = DQValidation(
        spark_session,
        table_name,
        schema_name="dq",
        run_month="",
        dq_check_folder="test/data",
        local=True,
    )

    assert dq_validation.checks(functional=True) == validation_output
    assert caplog.messages[-1] == expected_logging


@pytest.mark.parametrize(
    ("main_data", "validation_output", "expected_logging"),
    [
        (
            "dq_test_happy",
            True,
            ["Number of columns matches: 4 expected and received", "Datatypes match"],
        ),
        (
            "dq_test_happy_col_num",
            True,
            [
                "Number of columns matches: 4 expected and received",
                "No datatypes checked",
            ],
        ),
        (
            "dq_test_no_checks",
            None,
            ["No columns to check"],
        ),
        (
            "dq_test_unhappy_col_num",
            False,
            [
                "Number of columns incorrect: expected 5, received 4",
                "No datatypes checked",
            ],
        ),
        (
            "dq_test_unhappy_num_cols",
            False,
            [
                "Number of columns incorrect: expected 5, received 4",
                "Mismatch in datatypes, differences: "
                "expected [('Extra', 'int')], received []",
            ],
        ),
        (
            "dq_test_unhappy_type_cols",
            False,
            [
                "Number of columns matches: 4 expected and received",
                "Mismatch in datatypes, differences: "
                "expected [('Type', 'int')], received [('Type', 'string')]",
            ],
        ),
    ],
    indirect=["main_data"],
)
def test_columns(
    spark_session,
    main_data,
    reference_data,
    validation_output,
    expected_logging,
    caplog,
):
    """Test number of columns and datatypes for a given table"""
    table_name = main_data[0]

    dq_validation = DQValidation(
        spark_session,
        table_name,
        dq_check_folder="test/data",
        schema_name="dq",
        run_month="",
        local=True,
    )

    assert dq_validation.columns_datatypes().get("result", None) == validation_output
    assert caplog.messages[-len(expected_logging) :] == expected_logging


@pytest.mark.parametrize(
    ("main_data", "validation_output", "expected_logging"),
    [
        (
            "dq_test_happy",
            True,
            [
                "['PK1', 'PK2'] is unique",
                "No nulls found in ['PK1', 'PK2']",
                "Primary key ['PK1', 'PK2'] validated successfully",
            ],
        ),
        (
            "dq_test_no_check",
            None,
            ["No Primary Key to check"],
        ),
        (
            "dq_test_unhappy_pk_dup",
            False,
            [
                "Duplicates found in ['PK1']: [Row(PK1=2, count=2)]",
                "No nulls found in ['PK1']",
                "Issues found in primary key ['PK1']",
            ],
        ),
        (
            "dq_test_unhappy_pk_null",
            False,
            [
                "['PK1', 'PK2', 'PK3'] is unique",
                "Nulls found in not nullable column(s): ['PK3']",
                "Issues found in primary key ['PK1', 'PK2', 'PK3']",
            ],
        ),
    ],
    indirect=["main_data"],
)
def test_primary_key(
    spark_session,
    main_data,
    reference_data,
    validation_output,
    expected_logging,
    caplog,
):
    """Test primary key nulls and uniqueness for a given table"""

    table_name = main_data[0]

    dq_validation = DQValidation(
        spark_session,
        table_name,
        schema_name="dq",
        run_month="",
        dq_check_folder="test/data",
        local=True,
    )

    assert dq_validation.primary_key().get("result", None) == validation_output
    assert caplog.messages[-len(expected_logging) :] == expected_logging


@pytest.mark.parametrize(
    ("main_data", "validation_output", "expected_logging"),
    [
        (
            "dq_test_happy",
            True,
            ["No nulls found in ['PK1']"],
        ),
        (
            "dq_test_no_check",
            None,
            ["No not nullable columns to check"],
        ),
        (
            "dq_test_unhappy_not_null",
            False,
            ["Nulls found in not nullable column(s): ['Type']"],
        ),
    ],
    indirect=["main_data"],
)
def test_not_nulls(
    spark_session,
    main_data,
    reference_data,
    validation_output,
    expected_logging,
    caplog,
):
    """Test not nullable columns for a given table"""

    table_name = main_data[0]

    dq_validation = DQValidation(
        spark_session,
        table_name,
        schema_name="dq",
        run_month="",
        dq_check_folder="test/data",
        local=True,
    )

    assert dq_validation.not_null().get("result", None) == validation_output
    assert caplog.messages[-len(expected_logging) :] == expected_logging


@pytest.mark.parametrize(
    ("main_data", "validation_output", "expected_logging"),
    [
        (
            "dq_test_happy",
            True,
            [
                "Referential check successful, all values from Type "
                "with filter 'Type is not null' are present in reference.Lookup1"
            ],
        ),
        (
            "dq_test_no_check",
            None,
            ["No referential integrity checks"],
        ),
        (
            "dq_test_unhappy_ref",
            False,
            [
                "Referential check failed, not all values from Type "
                "with filter 'Type is not null' are present in reference.Lookup2\n"
                "1 values not available: ['B']"
            ],
        ),
        (
            "dq_test_unhappy_ref_filter",
            False,
            [
                "Referential check failed, not all values from Type are present in reference.Lookup1\n1 values not available: [None]"  # noqa: E501
            ],
        ),
    ],
    indirect=["main_data"],
)
def test_referential_integrity(
    spark_session,
    main_data,
    reference_data,
    validation_output,
    expected_logging,
    caplog,
):
    """Test referential integrity for a given table"""

    table_name = main_data[0]

    dq_validation = DQValidation(
        spark_session,
        table_name,
        schema_name="dq",
        run_month="",
        dq_check_folder="test/data",
        local=True,
    )

    assert (
        dq_validation.referential_integrity().get("result", None) == validation_output
    )
    assert caplog.messages[-len(expected_logging) :] == expected_logging


@pytest.mark.parametrize(
    ("main_data", "validation_output", "expected_logging"),
    [
        (
            "dq_test_happy",
            True,
            ["['PK2'] is unique"],
        ),
        (
            "dq_test_no_check",
            None,
            ["No unique columns to check"],
        ),
        (
            "dq_test_unhappy_unique",
            False,
            ["Duplicates found in ['PK1']: [Row(PK1=2, count=2)]", "['PK2'] is unique"],
        ),
    ],
    indirect=["main_data"],
)
def test_unique(
    spark_session,
    main_data,
    validation_output,
    expected_logging,
    caplog,
):
    """Test full DQ validation for a given table"""

    table_name = main_data[0]

    dq_validation = DQValidation(
        spark_session,
        table_name,
        schema_name="dq",
        dq_check_folder="test/data",
        run_month="",
        local=True,
    )

    assert (
        dq_validation.unique(individual=True).get("result", None) == validation_output
    )
    assert caplog.messages[-len(expected_logging) :] == expected_logging


@pytest.mark.parametrize(
    ("main_data", "source_system", "log_range"),
    [
        ("dq_test_fr_happy", "fr", [*list(range(2, 10)), 11]),
        ("dq_test_fr_no_generic", "fr", [*list(range(1, 9)), 10, 11]),
        ("dq_test_nospecific_happy", "nospecific", [0, *list(range(2, 9)), 10, 11]),
    ],
    indirect=["main_data"],
)
def test_generic_checks(
    spark_session, main_data, reference_data, source_system, log_range, caplog
):
    table_name = main_data[0]

    log = [
        "No specific checks file available for "
        "dq.dq_test_nospecific_happy at "
        + str(
            Path(
                MAPPING_ROOT_DIR
                / "test"
                / "data"
                / "dq"
                / "dq_test_nospecific_happy.yml"
            )
        ),
        "No generic checks file available for "
        "dq.dq_test_fr_no_generic at "
        + str(
            Path(
                MAPPING_ROOT_DIR / "test" / "data" / "dq" / "dq_test_[]_no_generic.yml"
            )
        ),
        "Number of columns matches: 4 expected and received",
        "Datatypes match",
        "['PK1', 'PK2'] is unique",
        "No nulls found in ['PK1', 'PK2']",
        "Primary key ['PK1', 'PK2'] validated successfully",
        "No nulls found in ['PK1']",
        "['PK2'] is unique",
        "Referential check successful, "
        "all values from Type with filter 'Type is not null' "
        "are present in reference.Lookup1",
        "No referential integrity checks",
        "Checks completed successfully",
    ]

    log = [log[i] for i in log_range]

    dq_validation = DQValidation(
        spark_session,
        table_name,
        schema_name="dq",
        dq_check_folder="test/data",
        run_month="",
        source_system=source_system,
        local=True,
    )

    assert dq_validation.checks(functional=True)
    assert caplog.messages == log
