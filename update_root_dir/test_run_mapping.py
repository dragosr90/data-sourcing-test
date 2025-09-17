from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from abnamro_bsrc_etl.scripts.run_mapping import run_mapping
from test.scripts.assert_utils import get_assert_calls_args


# Fixtures
@pytest.fixture
def mock_get_env(mocker):
    """Fixture to mock get_env."""
    return mocker.patch("abnamro_bsrc_etl.utils.get_env.get_env")


@pytest.fixture
def mock_parse_yaml(mocker):
    """Fixture to mock parse_yaml."""
    return mocker.patch("abnamro_bsrc_etl.scripts.run_mapping.parse_yaml")


@pytest.fixture
def mock_validate(mocker):
    """Fixture to mock validate_business_logic_mapping."""
    return mocker.patch(
        "abnamro_bsrc_etl.scripts.run_mapping.validate_business_logic_mapping"
    )


@pytest.fixture
def mock_write_and_comment(mocker):
    """Fixture to mock write_and_comment."""
    return mocker.patch("abnamro_bsrc_etl.scripts.run_mapping.write_and_comment")


@pytest.fixture
def mock_integration_class(mocker):
    """Fixture to mock GetIntegratedData class."""
    return mocker.patch("abnamro_bsrc_etl.scripts.run_mapping.GetIntegratedData")


@pytest.fixture
def mock_transform_business_logic(mocker):
    """Fixture to mock transform_business_logic_sql."""
    return mocker.patch(
        "abnamro_bsrc_etl.scripts.run_mapping.transform_business_logic_sql"
    )


@pytest.fixture
def mock_write_to_log(mock_write_to_log):
    """Fixture to mock write_to_log."""
    return mock_write_to_log("run_mapping")


@pytest.fixture
def mock_datetime_now(mocker):
    """Fixture to mock datetime.now to return a fixed datetime."""
    return mocker.patch("abnamro_bsrc_etl.scripts.run_mapping.datetime")


# Helper function to set up mock data
def setup_mock_data(
    mock_datetime_now,
    mock_get_env,
    mock_validate,
    mock_integration_class,
    mock_parse_yaml,
    mock_transform_business_logic,
    mock_write_and_comment,
    *,
    validation_output,
    write_and_comment_output,
):
    """Set up mock data for tests."""

    # Generate fixed datetime since current timestamp is changing
    mocked_dt = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime_now.now.return_value = mocked_dt

    # Set outputs of several mocked methods
    mock_get_env.return_value = "test"

    # The return value of mocked validate_business_logic_mapping function.
    # True or False, so the behaviour of both scenarios are tested
    mock_validate.return_value = validation_output

    business_logic = {
        "description": "test_mapping",
        "sources": [{"alias": "TBL_MOCK", "source": "test_source"}],
    }
    mock_parse_yaml.return_value = business_logic

    # Mock the instance of GetIntegratedData
    mock_integration_instance = MagicMock()
    mock_integration_class.return_value = mock_integration_instance

    # Assign data and transformed data so the output can be asserted
    data = MagicMock()
    transformed_data = MagicMock()
    mock_integration_instance.get_integrated_data.return_value = data
    mock_transform_business_logic.return_value = transformed_data

    # The return value of mocked write_and_comment function.
    # True or False, so the behaviour of both scenarios are tested
    mock_write_and_comment.return_value = write_and_comment_output

    return mocked_dt, business_logic, data, transformed_data


def assert_write_to_log_calls(
    mock_write_to_log, mock_spark, run_month, gen_log_info, statuses, comments
):
    """Helper function to assert write_to_log calls."""
    calls = [
        {
            "spark": mock_spark,
            "run_month": run_month,
            "log_table": "process_log",
            "record": {
                **gen_log_info,
                "Status": status,
                "Comments": comment,
            },
        }
        for status, comment in zip(statuses, comments, strict=False)
    ]
    # Assert that specific calls with input arguments are executed
    write_to_log_calls = get_assert_calls_args(calls)
    mock_write_to_log.assert_has_calls(**write_to_log_calls)
    # Assert that only these calls are executed
    assert mock_write_to_log.call_count == len(calls)


def get_gen_log_info(run_id, timestamp, delivery_entity):
    """Get generic log info."""
    return {
        "RunID": run_id,
        "Timestamp": timestamp,
        "Workflow": "",
        "Component": "data/TEST_YAML.yml",
        "Layer": "test",
        "SourceSystem": delivery_entity,
    }


@pytest.mark.parametrize("run_month", ["202412", "202504"])
@pytest.mark.parametrize("delivery_entity", ["IHUB-BE1", "", "FBS"])
@pytest.mark.parametrize("run_id", [1, 5])
def test_run_mapping(
    mock_datetime_now,
    mock_spark,
    mock_get_env,
    mock_validate,
    mock_parse_yaml,
    mock_integration_class,
    mock_transform_business_logic,
    mock_write_and_comment,
    run_month,
    delivery_entity,
    run_id,
    mock_write_to_log,
):
    """Test the run_mapping function."""
    mocked_dt, business_logic, data, transformed_data = setup_mock_data(
        mock_datetime_now,
        mock_get_env,
        mock_validate,
        mock_integration_class,
        mock_parse_yaml,
        mock_transform_business_logic,
        mock_write_and_comment,
        validation_output=True,
        write_and_comment_output=True,
    )

    # Call the function under test
    run_mapping(
        mock_spark,
        run_month=run_month,
        stage="test",
        target_mapping="data/TEST_YAML.yml",
        delivery_entity=delivery_entity,
        run_id=run_id,
    )

    # Assertions
    mock_integration_class.assert_called_once_with(
        spark=mock_spark, business_logic=business_logic
    )
    mock_transform_business_logic.assert_called_once_with(
        data=data, business_logic=business_logic
    )
    mock_write_and_comment.assert_called_once_with(
        spark=mock_spark,
        business_logic=business_logic,
        data=transformed_data,
        run_month=run_month,
        dq_check_folder="dq_checks",
        source_system=delivery_entity,
        local=False,
    )

    gen_log_info = get_gen_log_info(run_id, mocked_dt, delivery_entity)

    assert_write_to_log_calls(
        mock_write_to_log,
        mock_spark,
        run_month,
        gen_log_info,
        statuses=["Started", "Completed"],
        comments=["", ""],
    )


@pytest.mark.parametrize("run_month", ["202412", "202504"])
@pytest.mark.parametrize("delivery_entity", ["IHUB-BE1", "", "FBS"])
@pytest.mark.parametrize("run_id", [1, 5])
@pytest.mark.parametrize(
    ("validation_output", "write_and_comment_output", "fail_comments", "exit_code"),
    [
        (False, True, "Mapping validation failed", 1),
        (True, False, "DQ validation failed", 2),
    ],
)
def test_run_mapping_exit(
    mock_datetime_now,
    mock_spark,
    mock_get_env,
    mock_validate,
    mock_parse_yaml,
    mock_integration_class,
    mock_transform_business_logic,
    mock_write_and_comment,
    run_month,
    delivery_entity,
    run_id,
    mock_write_to_log,
    validation_output,
    write_and_comment_output,
    fail_comments,
    exit_code,
):
    """Test the run_mapping function for exit scenarios."""
    mocked_dt, business_logic, data, transformed_data = setup_mock_data(
        mock_datetime_now,
        mock_get_env,
        mock_validate,
        mock_integration_class,
        mock_parse_yaml,
        mock_transform_business_logic,
        mock_write_and_comment,
        validation_output=validation_output,
        write_and_comment_output=write_and_comment_output,
    )

    # Call the function under test and expect a SystemExit
    with pytest.raises(SystemExit) as exc_info:
        run_mapping(
            mock_spark,
            run_month=run_month,
            stage="test",
            target_mapping="data/TEST_YAML.yml",
            delivery_entity=delivery_entity,
            run_id=run_id,
        )

    # Assertions based on validation output
    if validation_output:
        mock_integration_class.assert_called_once_with(
            spark=mock_spark, business_logic=business_logic
        )
        mock_transform_business_logic.assert_called_once_with(
            data=data, business_logic=business_logic
        )
        mock_write_and_comment.assert_called_once_with(
            spark=mock_spark,
            business_logic=business_logic,
            data=transformed_data,
            run_month=run_month,
            dq_check_folder="dq_checks",
            source_system=delivery_entity,
            local=False,
        )
    else:
        mock_integration_class.assert_not_called()
        mock_transform_business_logic.assert_not_called()
        mock_write_and_comment.assert_not_called()

    gen_log_info = get_gen_log_info(run_id, mocked_dt, delivery_entity)

    assert_write_to_log_calls(
        mock_write_to_log,
        mock_spark,
        run_month,
        gen_log_info,
        statuses=["Started", "Failed"],
        comments=["", fail_comments],
    )

    assert exc_info.value.code == exit_code
