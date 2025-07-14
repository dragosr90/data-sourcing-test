import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import call

import pytest
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config.exceptions import NonSSFExtractionError
from src.staging.extract_nonssf_data import ExtractNonSSFData
from src.staging.status import NonSSFStepStatus


class FileInfoMock(dict):
    """Allows to access dictionary keys as attributes.
    And adds a simple isDir method."""

    __getattr__ = dict.get

    def isDir(self):  # noqa: N802
        return bool(self.name.endswith("/") or self.name.endswith(".parquet"))


# Fixtures for DRY compliance
@pytest.fixture
def metadata_schema():
    """Common schema for metadata DataFrame."""
    return StructType(
        [
            StructField("SourceSystem", StringType(), True),
            StructField("SourceFileName", StringType(), True),
            StructField("SourceFileFormat", StringType(), True),
            StructField("SourceFileDelimiter", StringType(), True),
            StructField("StgTableName", StringType(), True),
            StructField("FileDeliveryStep", IntegerType(), True),
            StructField("FileDeliveryStatus", StringType(), True),
        ]
    )


@pytest.fixture
def metadata_schema_with_deadline():
    """Schema for metadata DataFrame with Deadline column."""
    return StructType(
        [
            StructField("SourceSystem", StringType(), True),
            StructField("SourceFileName", StringType(), True),
            StructField("SourceFileFormat", StringType(), True),
            StructField("SourceFileDelimiter", StringType(), True),
            StructField("StgTableName", StringType(), True),
            StructField("FileDeliveryStep", IntegerType(), True),
            StructField("FileDeliveryStatus", StringType(), True),
            StructField("Deadline", DateType(), True),
        ]
    )


@pytest.fixture
def log_schema():
    """Common schema for log DataFrame."""
    return StructType(
        [
            StructField("SourceSystem", StringType(), True),
            StructField("SourceFileName", StringType(), True),
            StructField("DeliveryNumber", IntegerType(), True),
            StructField("FileDeliveryStep", IntegerType(), True),
            StructField("FileDeliveryStatus", StringType(), True),
            StructField("Result", StringType(), True),
            StructField("LastUpdatedDateTimestamp", TimestampType(), True),
            StructField("Comment", StringType(), True),
        ]
    )


@pytest.fixture
def empty_log_df(spark_session, log_schema):
    """Empty log DataFrame with schema."""
    return spark_session.createDataFrame([], schema=log_schema)


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_extract_non_ssf_data(
    spark_session,
    mocker,
    run_month,
    source_container,
    caplog,
):
    test_container = f"abfss://{source_container}@bsrcdadls.dfs.core.windows.net"
    month_container = f"abfss://{run_month}@bsrcdadls.dfs.core.windows.net"
    metadata_path = f"bsrc_d.metadata_{run_month}.metadata_nonssf"
    log_path = f"bsrc_d.log_{run_month}.log_nonssf"

    # Create a mock DataFrame
    schema_meta = [
        "SourceSystem",
        "SourceFileName",
        "SourceFileFormat",
        "SourceFileDelimiter",
        "StgTableName",
        "FileDeliveryStep",
        "FileDeliveryStatus",
    ]
    mock_meta = spark_session.createDataFrame(
        [
            (
                "lrd_static",
                "TEST_NON_SSF_V1",
                ".txt",
                "|",
                "test_non_ssf_v1",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
            (
                "lrd_static",
                "TEST_NON_SSF_V2",
                ".txt",
                "|",
                "test_non_ssf_v2",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
            (
                "nme",
                "TEST_NON_SSF_V3",
                ".parquet",
                ",",
                "test_non_ssf_v3",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
            (
                "finob",
                "TEST_NON_SSF_V4",
                ".csv",
                ",",
                "test_non_ssf_v4",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=schema_meta,
    )

    schema_log = [
        "SourceSystem",
        "SourceFileName",
        "DeliveryNumber",
        "FileDeliveryStep",
        "FileDeliveryStatus",
        "Result",
        "LastUpdatedDateTimestamp",
        "Comment",
    ]
    mock_log = spark_session.createDataFrame(
        [
            (
                "LRD_STATIC",
                "TEST_NON_SSF_V1",
                1,
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                "Success",
                datetime.now(timezone.utc),
                "Test comment",
            )
        ],
        schema=schema_log,
    )

    dummy_df = spark_session.createDataFrame(
        [(1, "2", 3)],
        schema=StructType(
            [
                StructField("col1", IntegerType()),
                StructField("col2", StringType()),
                StructField("col3", IntegerType()),
            ]
        ),
    )

    # Mock spark.read.json and spark.read.table to return the mock DataFrames
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [
        mock_meta,
        mock_log,
        dummy_df,
        dummy_df,
        dummy_df,
        dummy_df,
    ]

    mock_write = mocker.patch("pyspark.sql.DataFrameWriter.parquet")
    mock_save_table = mocker.patch("pyspark.sql.DataFrameWriter.saveAsTable")

    # Check ExtractNonSSFData class initialisation
    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Verify that spark.read.table was called with the correct arguments
    mock_read.table.assert_any_call(f"bsrc_d.metadata_{run_month}.metadata_nonssf")
    mock_read.table.assert_any_call(f"bsrc_d.log_{run_month}.log_nonssf")

    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")
    # Add more side effects to handle all the ls calls
    effect = [
        # First round of ls calls for get_all_files
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/{folder}/{file}",
                    "name": f"{file}",
                }
            )
            for file, folder in li
        ]
        for li in [
            [
                ("TEST_NON_SSF_V3.parquet", "NME"),
                ("TEST_NON_SSF_V3.csv", "NME"),  # Wrong extension file
                ("processed/", "NME"),
            ],
            [("TEST_NON_SSF_V4.csv", "FINOB"), ("processed/", "FINOB")],
            [
                ("TEST_NON_SSF_V1.txt", "LRD_STATIC"),
                ("TEST_NON_SSF_V5.txt", "LRD_STATIC"),  # Not in metadata
                ("processed/", "LRD_STATIC"),
            ],
            # For place_static_data - V2 is missing, check processed folder
            [("TEST_NON_SSF_V2_999999.txt", "LRD_STATIC/processed")],
            # Additional ls call to check if the file exists
            [("TEST_NON_SSF_V2_999999.txt", "LRD_STATIC/processed")],
        ]
    ]
    mock_dbutils_fs_ls.side_effect = effect
    mock_dbutils_fs_cp = mocker.patch.object(extraction.dbutils.fs, "cp")
    mock_dbutils_fs_mv = mocker.patch.object(extraction.dbutils.fs, "mv")

    # Test with deadline passed (default behavior in the test)
    found_files = extraction.get_all_files(deadline_passed=True)
    mock_dbutils_fs_cp.assert_any_call(
        f"{test_container}/LRD_STATIC/processed/TEST_NON_SSF_V2_999999.txt",
        f"{test_container}/LRD_STATIC/TEST_NON_SSF_V2.txt",
    )

    assert (
        "File TEST_NON_SSF_V5 not found in metadata. "
        "Please check if it should be delivered." in caplog.messages
    )

    mock_read.csv.return_value = dummy_df
    mock_read.parquet.return_value = dummy_df

    for file in found_files:
        file_name = file["file_name"]
        file_name_base = Path(file_name).name
        file_name_no_ext = Path(file_name).stem
        file_name_ext = Path(file_name).suffix
        source_system = file["source_system"]

        result = file_name_base != "TEST_NON_SSF_V3.csv"
        assert extraction.initial_checks(**file) is result

        if file_name_base == "TEST_NON_SSF_V3.csv":
            continue

        assert extraction.convert_to_parquet(**file)

        if source_system == "LRD_STATIC" or source_system == "FINOB":
            sep = {"LRD_STATIC": "|", "FINOB": ","}.get(source_system, ",")
            mock_read.csv.assert_any_call(
                f"{test_container}/{source_system}/{file_name_base}",
                sep=sep,
                header=True,
            )
        else:
            mock_read.parquet.assert_any_call(
                f"{test_container}/{source_system}/{file_name_base}",
            )
        mock_write.assert_any_call(
            f"{month_container}/sourcing_landing_data/NON_SSF/{source_system}/{file_name_no_ext}.parquet",
            mode="overwrite",
        )

        assert extraction.move_source_file(**file)

        calls = mock_dbutils_fs_mv.call_args_list
        assert any(
            args[0] == f"{test_container}/{source_system}/{file_name_base}"
            and re.match(
                rf"{test_container}/{source_system}/processed/{file_name_no_ext}__\d{{8}}\d{{6}}{file_name_ext}",
                args[1],
            )
            for args, _ in calls
        )

        data = extraction.extract_from_parquet(file["source_system"], file["file_name"])
        stg_table_name = extraction.get_staging_table_name(file["file_name"])
        assert extraction.save_to_stg_table(
            data=data,
            stg_table_name=stg_table_name,
            **file,
        )
        mock_save_table.assert_any_call(
            f"bsrc_d.stg_{run_month}.{file_name_no_ext.lower()}"
        )

        assert extraction.validate_data_quality(
            **file,
            stg_table_name=file_name_no_ext.lower(),
        )

    # Count update metadata and log calls
    metadata_path_calls, log_path_calls = (
        len(
            [
                call_args
                for call_args in mock_save_table.call_args_list
                if call_args == call(path)
            ]
        )
        for path in [metadata_path, log_path]
    )

    # For every file (v1 - v4) we log every step from received + 1 - 5: (4 x 6)
    # + 2 steps for v3.wrong_extension (received and initial checks)
    # So in total 26 calls for metadata and log
    assert metadata_path_calls == 26
    assert log_path_calls == 26


@pytest.mark.parametrize(
    ("run_month", "source_container", "deadline_passed"),
    [
        ("202503", "test-container", True),
        ("202503", "test-container", False),
    ],
)
def test_extract_non_ssf_data_with_deadline(
    spark_session,
    mocker,
    run_month,
    source_container,
    deadline_passed,
    metadata_schema,
    empty_log_df,
    caplog,
):
    """Test the deadline functionality for LRD_STATIC files."""
    test_container = f"abfss://{source_container}@bsrcdadls.dfs.core.windows.net"

    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "lrd_static",
                "TEST_STATIC_FILE",
                ".txt",
                "|",
                "test_static_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    # Create extraction instance
    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock filesystem operations
    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")
    mock_dbutils_fs_cp = mocker.patch.object(extraction.dbutils.fs, "cp")

    # Set up the mock file system - file is missing from LRD_STATIC but exists in processed # noqa: E501
    effect = [
        [],  # Empty NME folder
        [],  # Empty FINOB folder
        [],  # Empty LRD_STATIC folder (file not delivered)
        [  # Processed folder contains the file
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/TEST_STATIC_FILE_20240101.txt",  # noqa: E501
                    "name": "TEST_STATIC_FILE_20240101.txt",
                }
            )
        ],
        [  # Second call to processed folder for ls check
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/TEST_STATIC_FILE_20240101.txt",  # noqa: E501
                    "name": "TEST_STATIC_FILE_20240101.txt",
                }
            )
        ],
    ]
    mock_dbutils_fs_ls.side_effect = effect

    # Set deadline date
    deadline_date = (
        datetime.now(timezone.utc) - timedelta(days=1)
        if deadline_passed
        else datetime.now(timezone.utc) + timedelta(days=1)
    )

    # Call get_all_files with deadline information
    found_files = extraction.get_all_files(
        deadline_passed=deadline_passed, deadline_date=deadline_date
    )

    if deadline_passed:
        # Should copy the file from processed folder
        mock_dbutils_fs_cp.assert_called_once_with(
            f"{test_container}/LRD_STATIC/processed/TEST_STATIC_FILE_20240101.txt",
            f"{test_container}/LRD_STATIC/TEST_STATIC_FILE.txt",
        )
        # Check that the file was added to the list
        assert len(found_files) == 1
        assert found_files[0]["source_system"] == "LRD_STATIC"
        assert (
            found_files[0]["file_name"]
            == f"{test_container}/LRD_STATIC/TEST_STATIC_FILE.txt"
        )
        # Check log message
        assert "Deadline has passed" in caplog.text
        assert "LRD_STATIC files copied from processed folder" in caplog.text
    else:
        # Should NOT copy the file
        mock_dbutils_fs_cp.assert_not_called()
        # No files should be found
        assert len(found_files) == 0
        # Check log messages
        assert (
            "File TEST_STATIC_FILE not delivered but deadline not reached yet"
            in caplog.text
        )
        assert "Deadline not yet reached" in caplog.text
        assert "LRD_STATIC files will not be copied" in caplog.text


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_place_static_data_individual_deadlines(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema_with_deadline,
    empty_log_df,
    caplog,
):
    """Test place_static_data processes files based on overall deadline."""
    test_container = f"abfss://{source_container}@bsrcdadls.dfs.core.windows.net"

    # Current time for comparison
    current_time = datetime.now(timezone.utc)
    yesterday = (current_time - timedelta(days=1)).date()
    tomorrow = (current_time + timedelta(days=1)).date()

    # Create mock metadata with different deadline scenarios
    mock_meta = spark_session.createDataFrame(
        [
            # File 1: Deadline passed
            (
                "lrd_static",
                "FILE_PAST_DEADLINE",
                ".txt",
                "|",
                "test_file1",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                yesterday,
            ),
            # File 2: Deadline not passed
            (
                "lrd_static",
                "FILE_FUTURE_DEADLINE",
                ".txt",
                "|",
                "test_file2",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                tomorrow,
            ),
            # File 3: No deadline
            (
                "lrd_static",
                "FILE_NO_DEADLINE",
                ".txt",
                "|",
                "test_file3",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                None,
            ),
        ],
        schema=metadata_schema_with_deadline,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock filesystem operations
    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")
    mock_dbutils_fs_cp = mocker.patch.object(extraction.dbutils.fs, "cp")

    # Set up ls responses - all files exist in processed folder
    mock_dbutils_fs_ls.side_effect = [
        # FILE_NO_DEADLINE check
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/FILE_NO_DEADLINE_20240101.txt",  # noqa: E501
                    "name": "FILE_NO_DEADLINE_20240101.txt",
                }
            )
        ],
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/FILE_NO_DEADLINE_20240101.txt",  # noqa: E501
                    "name": "FILE_NO_DEADLINE_20240101.txt",
                }
            )
        ],
        # FILE_FUTURE_DEADLINE check
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/FILE_FUTURE_DEADLINE_20240101.txt",  # noqa: E501
                    "name": "FILE_FUTURE_DEADLINE_20240101.txt",
                }
            )
        ],
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/FILE_FUTURE_DEADLINE_20240101.txt",  # noqa: E501
                    "name": "FILE_FUTURE_DEADLINE_20240101.txt",
                }
            )
        ],
        # FILE_PAST_DEADLINE check
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/FILE_PAST_DEADLINE_20240101.txt",  # noqa: E501
                    "name": "FILE_PAST_DEADLINE_20240101.txt",
                }
            )
        ],
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/FILE_PAST_DEADLINE_20240101.txt",  # noqa: E501
                    "name": "FILE_PAST_DEADLINE_20240101.txt",
                }
            )
        ],
    ]

    # Mock saveAsTable to prevent actual writes
    mocker.patch("pyspark.sql.DataFrameWriter.saveAsTable")

    # Call place_static_data with overall deadline passed
    extraction.place_static_data([], deadline_passed=True)

    # When deadline_passed=True, all expected files should be copied
    assert mock_dbutils_fs_cp.call_count == 3

    # Check that all files were copied
    calls = mock_dbutils_fs_cp.call_args_list
    copied_files = [call[0][1].split("/")[-1] for call in calls]

    assert "FILE_NO_DEADLINE.txt" in copied_files
    assert "FILE_FUTURE_DEADLINE.txt" in copied_files
    assert "FILE_PAST_DEADLINE.txt" in copied_files


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_place_static_data_keyword_only(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
):
    """Test that place_static_data requires deadline_passed as keyword argument."""
    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "lrd_static",
                "TEST_FILE",
                ".txt",
                "|",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    # Create extraction instance
    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock the dbutils.fs.ls to prevent actual filesystem access
    mocker.patch.object(extraction.dbutils.fs, "ls", return_value=[])

    # Test that calling with positional argument raises TypeError
    with pytest.raises(
        TypeError, match="takes 2 positional arguments but 3 were given"
    ):
        extraction.place_static_data([], True)

    # Test that calling with keyword argument works
    result = extraction.place_static_data([], deadline_passed=True)  # This should work
    assert isinstance(result, list)


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_check_deadline_violations_with_dates(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema_with_deadline,
    empty_log_df,
    caplog,
):
    """Test check_deadline_violations includes deadline dates in error messages."""
    # Set deadline to yesterday to ensure it's passed
    yesterday = date.today() - timedelta(days=1)  # noqa: DTZ011

    mock_meta = spark_session.createDataFrame(
        [
            (
                "finob",
                "MISSING_FINOB_FILE",
                ".csv",
                ",",
                "test_finob",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                yesterday,
            ),
            (
                "nme",
                "MISSING_NME_FILE",
                ".parquet",
                ",",
                "test_nme",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                yesterday,
            ),
        ],
        schema=metadata_schema_with_deadline,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    # Create extraction instance
    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock write operations
    mock_save_table = mocker.patch("pyspark.sql.DataFrameWriter.saveAsTable")

    # Empty list of files (no files delivered)
    files_per_delivery_entity = []

    # Call check_deadline_violations - should raise exception
    with pytest.raises(NonSSFExtractionError) as exc_info:
        extraction.check_deadline_violations(files_per_delivery_entity)

    # Check that the error message includes deadline dates
    error_msg = str(exc_info.value)
    # The error message format includes "deadline: YYYY-MM-DD HH:MM:SS UTC"
    assert f"deadline: {yesterday} 00:00:00 UTC" in error_msg
    assert "Missing files after deadline" in error_msg
    # Verify both files are mentioned
    assert "finob/MISSING_FINOB_FILE" in error_msg
    assert "nme/MISSING_NME_FILE" in error_msg

    # Check log messages include deadline dates for both files
    assert (
        f"Deadline passed ({yesterday} 00:00:00 UTC): Missing expected file MISSING_FINOB_FILE from finob"  # noqa: E501
        in caplog.text
    )
    assert (
        f"Deadline passed ({yesterday} 00:00:00 UTC): Missing expected file MISSING_NME_FILE from nme"  # noqa: E501
        in caplog.text
    )

    # Verify update_log_metadata was called with deadline in comment
    # Check that saveAsTable was called for metadata updates
    metadata_calls = [
        call
        for call in mock_save_table.call_args_list
        if "metadata_nonssf" in str(call)
    ]
    # Should have 2 metadata updates - one for each missing file
    assert len(metadata_calls) == 2


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_check_deadline_violations_future_deadline(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema_with_deadline,
    empty_log_df,
):
    """Test that files with future deadlines don't trigger violations."""
    # Set deadline to tomorrow
    tomorrow = date.today() + timedelta(days=1)  # noqa: DTZ011

    mock_meta = spark_session.createDataFrame(
        [
            (
                "finob",
                "FUTURE_DEADLINE_FILE",
                ".csv",
                ",",
                "test_finob",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                tomorrow,
            ),
        ],
        schema=metadata_schema_with_deadline,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    # Create extraction instance
    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Empty list of files (no files delivered)
    files_per_delivery_entity = []

    # Call check_deadline_violations - should NOT raise exception
    # because deadline is in the future
    try:
        extraction.check_deadline_violations(files_per_delivery_entity)
    except NonSSFExtractionError:
        pytest.fail("Should not raise exception for future deadline")


# NEW TESTS FOR MISSING COVERAGE


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_place_static_data_copy_failure(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
):
    """Test place_static_data raises exception when copy fails."""
    test_container = f"abfss://{source_container}@bsrcdadls.dfs.core.windows.net"

    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "lrd_static",
                "TEST_FILE",
                ".txt",
                "|",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock filesystem operations
    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")
    mock_dbutils_fs_ls.return_value = [
        FileInfoMock(
            {
                "path": f"{test_container}/LRD_STATIC/processed/TEST_FILE_20240101.txt",
                "name": "TEST_FILE_20240101.txt",
            }
        )
    ]

    # Mock cp to raise OSError
    mock_dbutils_fs_cp = mocker.patch.object(extraction.dbutils.fs, "cp")
    mock_dbutils_fs_cp.side_effect = OSError("Permission denied")

    # Call place_static_data - should raise NonSSFExtractionError
    with pytest.raises(NonSSFExtractionError) as exc_info:
        extraction.place_static_data([], deadline_passed=True)

    assert "Failed to copy" in str(exc_info.value)
    assert "Permission denied" in str(exc_info.value)


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_place_static_data_with_non_expected_status(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
    caplog,
):
    """Test place_static_data skips files not in expected status."""
    # Create mock metadata DataFrame with non-expected status
    mock_meta = spark_session.createDataFrame(
        [
            (
                "lrd_static",
                "TEST_FILE",
                ".txt",
                "|",
                "test_file",
                NonSSFStepStatus.LOADED_STG.value,  # Use enum
                "Completed",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Call place_static_data with deadline passed
    result = extraction.place_static_data([], deadline_passed=True)
    assert "File TEST_FILE is not in expected status" in caplog.text
    assert len(result) == 0


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_place_static_data_with_redelivery_status(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
    caplog,
):
    """Test place_static_data processes files with REDELIVERY status."""
    test_container = f"abfss://{source_container}@bsrcdadls.dfs.core.windows.net"

    # Create mock metadata DataFrame with REDELIVERY status
    mock_meta = spark_session.createDataFrame(
        [
            (
                "lrd_static",
                "TEST_FILE",
                ".txt",
                "|",
                "test_file",
                NonSSFStepStatus.REDELIVERY.value,  # Use enum
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock filesystem operations
    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")

    # Return the processed file when checking the processed folder
    mock_dbutils_fs_ls.side_effect = [
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/TEST_FILE_20240101.txt",  # noqa: E501
                    "name": "TEST_FILE_20240101.txt",
                }
            )
        ],
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/TEST_FILE_20240101.txt",  # noqa: E501
                    "name": "TEST_FILE_20240101.txt",
                }
            )
        ],
    ]

    mock_dbutils_fs_cp = mocker.patch.object(extraction.dbutils.fs, "cp")
    # Also mock saveAsTable to prevent actual writes
    mocker.patch("pyspark.sql.DataFrameWriter.saveAsTable")

    # Call place_static_data with deadline passed - should process REDELIVERY files
    result = extraction.place_static_data([], deadline_passed=True)

    # Verify that the file was copied
    mock_dbutils_fs_cp.assert_called_once_with(
        f"{test_container}/LRD_STATIC/processed/TEST_FILE_20240101.txt",
        f"{test_container}/LRD_STATIC/TEST_FILE.txt",
    )
    assert len(result) == 1
    assert result[0] == f"{test_container}/LRD_STATIC/TEST_FILE.txt"


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_place_static_data_no_processed_files(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
    caplog,
):
    """Test place_static_data when no processed files are found."""
    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "lrd_static",
                "TEST_FILE",
                ".txt",
                "|",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock filesystem operations - empty processed folder
    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")
    mock_dbutils_fs_ls.return_value = []  # No files in processed folder

    # Call place_static_data with deadline passed
    result = extraction.place_static_data([], deadline_passed=True)
    assert (
        "File TEST_FILE not delivered and not found in LRD_STATIC/processed folder"
        in caplog.text
    )
    assert len(result) == 0


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_place_static_data_multiple_processed_files(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
    caplog,
):
    """Test place_static_data selects latest from multiple processed files."""
    test_container = f"abfss://{source_container}@bsrcdadls.dfs.core.windows.net"

    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "lrd_static",
                "TEST_FILE",
                ".txt",
                "|",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock filesystem operations with multiple processed files
    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")
    mock_dbutils_fs_ls.side_effect = [
        # Multiple processed files with different timestamps
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/TEST_FILE_20240101.txt",  # noqa: E501
                    "name": "TEST_FILE_20240101.txt",
                }
            ),
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/TEST_FILE_20240201.txt",  # noqa: E501
                    "name": "TEST_FILE_20240201.txt",
                }
            ),
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/TEST_FILE_20240301.txt",  # noqa: E501
                    "name": "TEST_FILE_20240301.txt",
                }
            ),
        ],
        # Second ls call returns single file
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/TEST_FILE_20240301.txt",  # noqa: E501
                    "name": "TEST_FILE_20240301.txt",
                }
            )
        ],
    ]

    mock_dbutils_fs_cp = mocker.patch.object(extraction.dbutils.fs, "cp")

    # Call place_static_data with deadline passed
    result = extraction.place_static_data([], deadline_passed=True)

    # Verify that the latest file was selected (max() function)
    mock_dbutils_fs_cp.assert_called_once_with(
        f"{test_container}/LRD_STATIC/processed/TEST_FILE_20240301.txt",
        f"{test_container}/LRD_STATIC/TEST_FILE.txt",
    )
    assert len(result) == 1
    assert "Copied TEST_FILE to static folder after deadline passed" in caplog.text


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_get_all_files_os_error(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
    caplog,
):
    """Test get_all_files handles OSError when accessing folders."""
    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "nme",
                "TEST_FILE",
                ".csv",
                ",",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock filesystem operations to raise OSError
    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")
    mock_dbutils_fs_ls.side_effect = [
        OSError("Permission denied for NME"),
        [],  # FINOB folder
        [],  # LRD_STATIC folder
    ]

    # Call get_all_files
    result = extraction.get_all_files()
    assert "Could not access folder NME: Permission denied for NME" in caplog.text
    assert len(result) == 0  # No files found due to error


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_get_all_files_filters_files_without_metadata(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
    caplog,
):
    """Test get_all_files excludes files without metadata match."""
    test_container = f"abfss://{source_container}@bsrcdadls.dfs.core.windows.net"

    # Create mock metadata DataFrame with only some files
    mock_meta = spark_session.createDataFrame(
        [
            (
                "nme",
                "EXPECTED_FILE",
                ".csv",
                ",",
                "expected_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
            (
                "finob",
                "ANOTHER_EXPECTED",
                ".csv",
                ",",
                "another_expected",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock filesystem operations - return files including ones not in metadata
    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")
    mock_dbutils_fs_ls.side_effect = [
        # NME folder - has both expected and unexpected files
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/NME/EXPECTED_FILE.csv",
                    "name": "EXPECTED_FILE.csv",
                }
            ),
            FileInfoMock(
                {
                    "path": f"{test_container}/NME/UNEXPECTED_FILE.csv",
                    "name": "UNEXPECTED_FILE.csv",
                }
            ),
            FileInfoMock(
                {
                    "path": f"{test_container}/NME/ANOTHER_UNEXPECTED.parquet",
                    "name": "ANOTHER_UNEXPECTED.parquet",
                }
            ),
        ],
        # FINOB folder
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/FINOB/ANOTHER_EXPECTED.csv",
                    "name": "ANOTHER_EXPECTED.csv",
                }
            ),
            FileInfoMock(
                {
                    "path": f"{test_container}/FINOB/NOT_IN_METADATA.csv",
                    "name": "NOT_IN_METADATA.csv",
                }
            ),
        ],
        # LRD_STATIC folder
        [],
    ]

    # Mock saveAsTable for metadata updates
    mocker.patch("pyspark.sql.DataFrameWriter.saveAsTable")

    # Call get_all_files
    result = extraction.get_all_files()

    # Verify that only files with metadata matches are included
    assert len(result) == 2

    file_names = [Path(f["file_name"]).stem for f in result]
    assert "EXPECTED_FILE" in file_names
    assert "ANOTHER_EXPECTED" in file_names
    assert "UNEXPECTED_FILE" not in file_names
    assert "ANOTHER_UNEXPECTED" not in file_names
    assert "NOT_IN_METADATA" not in file_names

    # Check warning messages for files not in metadata
    assert "File UNEXPECTED_FILE not found in metadata" in caplog.text
    assert "File ANOTHER_UNEXPECTED not found in metadata" in caplog.text
    assert "File NOT_IN_METADATA not found in metadata" in caplog.text


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_get_deadline_from_metadata_variations(
    spark_session,
    mocker,
    run_month,
    source_container,
    log_schema,
):
    """Test get_deadline_from_metadata with different deadline formats."""
    # Test with string deadline
    schema_meta = [
        "SourceSystem",
        "SourceFileName",
        "SourceFileFormat",
        "SourceFileDelimiter",
        "StgTableName",
        "FileDeliveryStep",
        "FileDeliveryStatus",
        "Deadline",
    ]
    mock_meta = spark_session.createDataFrame(
        [
            (
                "nme",
                "TEST_FILE",
                ".csv",
                ",",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                "2025-07-01",  # String deadline
            ),
        ],
        schema=schema_meta,
    )

    mock_log = spark_session.createDataFrame([], schema=log_schema)

    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, mock_log]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Test with string deadline
    deadline = extraction.get_deadline_from_metadata("nme", "TEST_FILE")
    assert deadline is not None
    assert deadline.strftime("%Y-%m-%d") == "2025-07-01"

    # Test with date object deadline
    mock_meta2 = spark_session.createDataFrame(
        [
            (
                "finob",
                "TEST_FILE2",
                ".csv",
                ",",
                "test_file2",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                date(2025, 7, 2),  # Date object
            ),
        ],
        schema=schema_meta,
    )
    mock_read.table.side_effect = [mock_meta2, mock_log]
    extraction2 = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )
    deadline2 = extraction2.get_deadline_from_metadata("finob", "TEST_FILE2")
    assert deadline2 is not None
    assert deadline2.strftime("%Y-%m-%d") == "2025-07-02"

    # Test with no deadline
    schema_with_deadline = StructType(
        [
            StructField("SourceSystem", StringType(), True),
            StructField("SourceFileName", StringType(), True),
            StructField("SourceFileFormat", StringType(), True),
            StructField("SourceFileDelimiter", StringType(), True),
            StructField("StgTableName", StringType(), True),
            StructField("FileDeliveryStep", IntegerType(), True),
            StructField("FileDeliveryStatus", StringType(), True),
            StructField("Deadline", DateType(), True),
        ]
    )

    mock_meta3 = spark_session.createDataFrame(
        [
            (
                "lrd_static",
                "TEST_FILE3",
                ".txt",
                "|",
                "test_file3",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                None,  # No deadline
            ),
        ],
        schema=schema_with_deadline,
    )
    mock_read.table.side_effect = [mock_meta3, mock_log]
    extraction3 = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )
    deadline3 = extraction3.get_deadline_from_metadata("lrd_static", "TEST_FILE3")
    assert deadline3 is None


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_get_deadline_from_metadata_no_rows(
    spark_session,
    mocker,
    run_month,
    source_container,
    empty_log_df,
):
    """Test get_deadline_from_metadata when no rows match."""
    # Create mock metadata DataFrame
    schema_meta = [
        "SourceSystem",
        "SourceFileName",
        "SourceFileFormat",
        "SourceFileDelimiter",
        "StgTableName",
        "FileDeliveryStep",
        "FileDeliveryStatus",
        "Deadline",
    ]
    mock_meta = spark_session.createDataFrame(
        [
            (
                "nme",
                "DIFFERENT_FILE",
                ".csv",
                ",",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                "2025-07-01",
            ),
        ],
        schema=schema_meta,
    )

    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Test with non-matching file
    deadline = extraction.get_deadline_from_metadata("nme", "NON_EXISTENT_FILE")
    assert deadline is None


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_convert_to_parquet_unsupported_format(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
    caplog,
):
    """Test convert_to_parquet with unsupported file format."""
    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "nme",
                "TEST_FILE",
                ".json",  # Unsupported format
                ",",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock the update_log_metadata method to verify it's called
    mock_update = mocker.patch.object(extraction, "update_log_metadata")

    # Test convert_to_parquet
    result = extraction.convert_to_parquet("NME", "TEST_FILE.json")
    assert result is False
    assert "Unsupported file format: .json" in caplog.text
    # Verify that update_log_metadata was called with FAILURE
    mock_update.assert_called_once()
    assert mock_update.call_args[1]["result"] == "FAILURE"


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_convert_to_parquet_with_export_failure(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
):
    """Test convert_to_parquet when export_to_parquet fails."""
    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "finob",
                "TEST_FILE",
                ".csv",
                ",",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock csv read to return a DataFrame
    dummy_df = spark_session.createDataFrame([(1, "test")], ["id", "value"])
    mock_read.csv.return_value = dummy_df

    # Mock export_to_parquet to return False
    mock_export = mocker.patch("src.staging.extract_nonssf_data.export_to_parquet")
    mock_export.return_value = False

    # Mock saveAsTable
    mock_save_table = mocker.patch("pyspark.sql.DataFrameWriter.saveAsTable")

    # Test convert_to_parquet
    result = extraction.convert_to_parquet("FINOB", "TEST_FILE.csv")
    assert result is False
    # Verify that update_log_metadata was called with FAILURE
    mock_save_table.assert_called()


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_convert_to_parquet_read_failure(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
    caplog,
):
    """Test convert_to_parquet when reading source file fails."""
    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "finob",
                "CORRUPT_FILE",
                ".csv",
                ",",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock csv read to fail
    mock_read.csv.side_effect = Exception("Corrupt file")

    # Test should raise the exception
    with pytest.raises(Exception, match="Corrupt file"):
        extraction.convert_to_parquet("FINOB", "CORRUPT_FILE.csv")


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_get_staging_table_name_missing_in_metadata(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
):
    """Test get_staging_table_name when file not in metadata."""
    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "nme",
                "EXISTING_FILE",
                ".csv",
                ",",
                "existing_table",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Test with non-existent file
    with pytest.raises(IndexError):
        extraction.get_staging_table_name("NON_EXISTENT_FILE.csv")


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_move_source_file_failure(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
):
    """Test move_source_file when move operation fails."""
    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "nme",
                "TEST_FILE",
                ".csv",
                ",",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock mv to raise exception
    mock_dbutils_fs_mv = mocker.patch.object(extraction.dbutils.fs, "mv")
    mock_dbutils_fs_mv.side_effect = Exception("File locked")

    # Mock saveAsTable
    mock_save_table = mocker.patch("pyspark.sql.DataFrameWriter.saveAsTable")

    # Test move_source_file
    result = extraction.move_source_file("NME", "TEST_FILE.csv")
    assert result is False
    # Verify that update_log_metadata was called with failure comment
    mock_save_table.assert_called()


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_check_deadline_violations_mixed_scenarios(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema_with_deadline,
    empty_log_df,
    caplog,
):
    """Test check_deadline_violations with mixed scenarios."""
    yesterday = date.today() - timedelta(days=1)  # noqa: DTZ011
    tomorrow = date.today() + timedelta(days=1)  # noqa: DTZ011

    mock_meta = spark_session.createDataFrame(
        [
            # File with passed deadline, not delivered
            (
                "finob",
                "MISSING_FILE",
                ".csv",
                ",",
                "test_finob",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                yesterday,
            ),
            # File with future deadline, not delivered (should not trigger violation)
            (
                "nme",
                "FUTURE_FILE",
                ".parquet",
                ",",
                "test_nme",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                tomorrow,
            ),
            # File with no deadline, not delivered (should not trigger violation)
            (
                "finob",
                "NO_DEADLINE_FILE",
                ".csv",
                ",",
                "test_finob2",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                None,
            ),
            # File with passed deadline as date
            (
                "nme",
                "STRING_DEADLINE_FILE",
                ".csv",
                ",",
                "test_nme2",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
                yesterday,  # Use date object instead of string
            ),
        ],
        schema=metadata_schema_with_deadline,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock write operations
    mock_save_table = mocker.patch("pyspark.sql.DataFrameWriter.saveAsTable")

    # Files delivered (only one file)
    files_per_delivery_entity = [
        {"source_system": "FINOB", "file_name": "NO_DEADLINE_FILE.csv"}
    ]

    # Call check_deadline_violations - should raise exception for 2 files
    with pytest.raises(NonSSFExtractionError) as exc_info:
        extraction.check_deadline_violations(files_per_delivery_entity)

    error_msg = str(exc_info.value)
    # Should include only files with passed deadlines
    assert "finob/MISSING_FILE" in error_msg
    assert "nme/STRING_DEADLINE_FILE" in error_msg
    # Should NOT include files with future or no deadlines
    assert "FUTURE_FILE" not in error_msg
    assert "NO_DEADLINE_FILE" not in error_msg
    # Check that exactly 2 metadata updates were made (for the 2 violations)
    metadata_calls = [
        call
        for call in mock_save_table.call_args_list
        if "metadata_nonssf" in str(call)
    ]
    assert len(metadata_calls) == 2


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_get_all_files_with_parquet_directory(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
):
    """Test get_all_files correctly handles parquet directories."""
    test_container = f"abfss://{source_container}@bsrcdadls.dfs.core.windows.net"

    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "nme",
                "PARQUET_DIR",
                ".parquet",
                ",",
                "parquet_dir",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock filesystem operations
    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")
    mock_dbutils_fs_ls.side_effect = [
        # NME folder with parquet directory
        [
            FileInfoMock(
                {
                    "path": f"{test_container}/NME/PARQUET_DIR.parquet",
                    "name": "PARQUET_DIR.parquet",
                    "isDir": lambda: True,  # This is a directory ending with .parquet
                }
            ),
            FileInfoMock(
                {
                    "path": f"{test_container}/NME/regular_file.csv",
                    "name": "regular_file.csv",
                    "isDir": lambda: False,
                }
            ),
        ],
        [],  # FINOB folder
        [],  # LRD_STATIC folder
    ]

    # Call get_all_files
    result = extraction.get_all_files()

    # Should include the parquet directory
    assert len(result) == 1
    assert result[0]["file_name"] == f"{test_container}/NME/PARQUET_DIR.parquet"
    assert result[0]["source_system"] == "NME"


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_save_to_stg_table_failure(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
):
    """Test save_to_stg_table failure handling."""
    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "nme",
                "TEST_FILE",
                ".csv",
                ",",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Create a dummy DataFrame
    dummy_df = spark_session.createDataFrame([(1, "test")], ["id", "value"])

    # Mock the saveAsTable to fail only on the first call (staging table write)
    mock_save_table = mocker.patch("pyspark.sql.DataFrameWriter.saveAsTable")
    mock_save_table.side_effect = [
        Exception("Write failed"),  # First call fails (staging table)
        None,  # Second call succeeds (log table)
        None,  # Third call succeeds (metadata table)
    ]

    # Test save_to_stg_table with failure
    result = extraction.save_to_stg_table(
        data=dummy_df,
        stg_table_name="test_table",
        source_system="NME",
        file_name="TEST_FILE.csv",
    )
    assert result is False


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_validate_data_quality_failure(
    spark_session,
    mocker,
    run_month,
    source_container,
    metadata_schema,
    empty_log_df,
):
    """Test validate_data_quality failure handling."""
    # Create mock metadata DataFrame
    mock_meta = spark_session.createDataFrame(
        [
            (
                "nme",
                "TEST_FILE",
                ".csv",
                ",",
                "test_file",
                NonSSFStepStatus.EXPECTED.value,
                "Expected",
            ),
        ],
        schema=metadata_schema,
    )

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, empty_log_df]

    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock DQValidation to raise exception
    mock_dq_validation = mocker.patch("src.staging.extract_base.DQValidation")
    mock_dq_validation.side_effect = Exception("Table not found")

    # Test validate_data_quality with failure
    result = extraction.validate_data_quality(
        source_system="NME",
        file_name="TEST_FILE.csv",
        stg_table_name="test_file",
    )
    assert result is False
