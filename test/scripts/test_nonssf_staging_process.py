from unittest.mock import MagicMock

import pytest

from abnamro_bsrc_etl.config.exceptions import NonSSFExtractionError
from abnamro_bsrc_etl.scripts.nonssf_staging_process import non_ssf_load
from abnamro_bsrc_etl.staging.extract_nonssf_data import ExtractNonSSFData
from test.scripts.assert_utils import (
    assert_before_and_after_failing_step,
    assert_write_to_log_calls,
    get_assert_calls_args,
    get_step_args,
)

NONSSF_STEPS = [
    "initial_checks",
    "convert_to_parquet",
    "move_source_file",
    "save_to_stg_table",
    "validate_data_quality",
]


@pytest.fixture
def mock_extraction(mock_extraction):
    """Generate mock_extraction fixture to use ExtractNonSSFData."""
    return mock_extraction(ExtractNonSSFData)


@pytest.fixture
def mock_write_to_log(mock_write_to_log):
    """Generate mock_write_to_log fixture to use ExtractNonSSFData."""
    return mock_write_to_log("nonssf_staging_process")


def setup_mock_data(
    mock_extraction,
    *,
    initial_checks=True,
    convert_to_parquet=True,
    move_source_file=True,
    save_to_stg_table=True,
    validate_data_quality=True,
):
    nme = {"source_system": "NME", "file_name": "file1.csv"}
    finob = {"source_system": "FINOB", "file_name": "file2.csv"}
    files = [nme, finob]
    mock_extraction.get_all_files.return_value = files
    mock_nme_df = MagicMock()
    mock_finob_df = MagicMock()
    mock_extraction.initial_checks.side_effect = [True, initial_checks]
    mock_extraction.convert_to_parquet.side_effect = [True, convert_to_parquet]
    mock_extraction.move_source_file.side_effect = [True, move_source_file]
    mock_extraction.extract_from_parquet.side_effect = [mock_nme_df, mock_finob_df]
    mock_extraction.get_staging_table_name.side_effect = ["stg_nme", "stg_finob"]
    mock_extraction.save_to_stg_table.side_effect = [True, save_to_stg_table]
    mock_extraction.validate_data_quality.side_effect = [True, validate_data_quality]
    return files, mock_nme_df, mock_finob_df


@pytest.mark.parametrize("run_month", ["202402", "202504"])
def test_non_ssf_load_success(
    mock_spark, mock_extraction, mock_write_to_log, run_month
):
    """Test successful execution of non_ssf_load."""
    files, mock_nme_df, mock_finob_df = setup_mock_data(mock_extraction)
    non_ssf_load(mock_spark, run_month=run_month, run_id=1)

    generic_calls = get_assert_calls_args(files)
    mock_extraction.get_all_files.assert_called_once_with()  # No parameters
    mock_extraction.initial_checks.assert_has_calls(**generic_calls)
    mock_extraction.convert_to_parquet.assert_has_calls(**generic_calls)
    mock_extraction.move_source_file.assert_has_calls(**generic_calls)
    mock_extraction.extract_from_parquet.assert_has_calls(**generic_calls)
    save_to_stg_table_calls = get_assert_calls_args(
        [
            {
                "file_name": "file1",
                "source_system": "NME",
                "stg_table_name": "stg_nme",
                "data": mock_nme_df,
            },
            {
                "file_name": "file2",
                "source_system": "FINOB",
                "stg_table_name": "stg_finob",
                "data": mock_finob_df,
            },
        ]
    )

    mock_extraction.save_to_stg_table.assert_has_calls(**save_to_stg_table_calls)
    validate_data_quality_calls = get_assert_calls_args(
        [
            {
                "file_name": "file1",
                "source_system": "NME",
                "stg_table_name": "stg_nme",
            },
            {
                "file_name": "file2",
                "source_system": "FINOB",
                "stg_table_name": "stg_finob",
            },
        ]
    )
    mock_extraction.validate_data_quality.assert_has_calls(
        **validate_data_quality_calls
    )
    # Retrieve all called paths of write_to_log
    # Overall (+1) and two single files (+2) = 3
    assert_write_to_log_calls(mock_write_to_log, started=3, completed=3, failed=0)


@pytest.mark.parametrize(
    "failing_step",
    NONSSF_STEPS,
)
@pytest.mark.parametrize(
    "run_month",
    ["202402", "202505"],
)
def test_non_ssf_load_failure(
    mock_spark,
    mock_extraction,
    mock_write_to_log,
    run_month,
    failing_step,
):
    """Test failure scenarios in non_ssf_load.

    This test verifies the behavior of the `non_ssf_load` function when one of the steps
    fails. It ensures that the process raises a `NonSSFExtractionError`,
    performs the correct assertions, and logs the appropriate status.

    Scenarios:
        - Each step in `NONSSF_STEPS` is tested as the failing step.
        - The process raises a `NonSSFExtractionError` when failing step is executed.
        - The process logs the appropriate status:
            - Started: Logs the start of the process and individual file processing.
            - Completed: Logs the completion of one file.
            - Failed: Logs the failure of the second file and the overall process.
        - Special handling for the failing step:
            - The `assert_before_and_after_failing_step` function verifies the behavior
              of steps before, including, and after the failing step.
            - The `fail_on_iteration` parameter is set to `1` to simulate the failure
              occurring during the second iteration.
    """
    step_args = get_step_args(failing_step=failing_step, steps=NONSSF_STEPS)
    files, _, _ = setup_mock_data(
        mock_extraction,
        **step_args,
    )
    with pytest.raises(NonSSFExtractionError):
        non_ssf_load(mock_spark, run_month=run_month, run_id=1)

    # Assertions with with correct parameters
    generic_calls = get_assert_calls_args(files)
    mock_extraction.get_all_files.assert_called_once_with()  # No parameters
    mock_extraction.initial_checks.assert_has_calls(**generic_calls)

    # Retrieve all called paths of write_to_log
    # Overall started (+1) and single files (+2) = 3
    # 2nd file failed so overall as well (+2)
    assert_write_to_log_calls(mock_write_to_log, started=3, completed=1, failed=2)

    assert_before_and_after_failing_step(
        mock_extraction,
        failing_step,
        NONSSF_STEPS,
        # In the setup mock data the second call is returned False, so iteration 1
        fail_on_iteration=1,
    )


def test_non_ssf_load_no_files(mock_spark, mock_extraction, mock_write_to_log):
    """Test scenario where no files are found."""
    mock_extraction.get_all_files.return_value = []  # No files found
    non_ssf_load(mock_spark, run_month="202301", run_id=1)

    mock_extraction.get_all_files.assert_called_once_with()  # No parameters
    assert_write_to_log_calls(mock_write_to_log, started=1, completed=1, failed=0)
