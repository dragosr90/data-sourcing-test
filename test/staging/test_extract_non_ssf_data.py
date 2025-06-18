import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import call

import pytest
from pyspark.sql.types import (
    IntegerType, 
    StringType, 
    StructField, 
    StructType,
    TimestampType,
)

from src.staging.extract_nonssf_data import ExtractNonSSFData


class FileInfoMock(dict):
    """Allows to access dictionary keys as attributes.
    And adds a simple isDir method."""

    __getattr__ = dict.get

    def isDir(self):  # noqa: N802
        return bool(self.name.endswith("/") or self.name.endswith(".parquet"))


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
                "LRD_STATIC",  # Changed to uppercase to match folder names
                "TEST_NON_SSF_V1",
                ".txt",
                "|",
                "test_non_ssf_v1",
                0,
                "Expected",
            ),
            (
                "LRD_STATIC",  # Changed to uppercase
                "TEST_NON_SSF_V2",
                ".txt",
                "|",
                "test_non_ssf_v2",
                0,
                "Expected",
            ),
            (
                "NME",  # Changed to uppercase
                "TEST_NON_SSF_V3",
                ".parquet",
                ",",
                "test_non_ssf_v3",
                0,
                "Expected",
            ),
            (
                "FINOB",  # Changed to uppercase
                "TEST_NON_SSF_V4",
                ".csv",
                ",",
                "test_non_ssf_v4",
                0,
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
                0,
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
    caplog,
):
    """Test the deadline functionality for LRD_STATIC files."""
    test_container = f"abfss://{source_container}@bsrcdadls.dfs.core.windows.net"

    # Create mock metadata DataFrame
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
                "LRD_STATIC",  # Uppercase to match code
                "TEST_STATIC_FILE",
                ".txt",
                "|",
                "test_static_file",
                0,
                "Expected",
            ),
        ],
        schema=schema_meta,
    )

    # Create schema for log DataFrame
    schema_log = StructType([
        StructField("SourceSystem", StringType(), True),
        StructField("SourceFileName", StringType(), True),
        StructField("DeliveryNumber", IntegerType(), True),
        StructField("FileDeliveryStep", IntegerType(), True),
        StructField("FileDeliveryStatus", StringType(), True),
        StructField("Result", StringType(), True),
        StructField("LastUpdatedDateTimestamp", TimestampType(), True),
        StructField("Comment", StringType(), True),
    ])
    
    # Create empty DataFrame with schema
    mock_log = spark_session.createDataFrame([], schema=schema_log)

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, mock_log]

    # Create extraction instance
    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock filesystem operations
    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")
    mock_dbutils_fs_cp = mocker.patch.object(extraction.dbutils.fs, "cp")
    
    # Set up the mock file system - file is missing from LRD_STATIC but exists in processed
    effect = [
        [],  # Empty NME folder
        [],  # Empty FINOB folder
        [],  # Empty LRD_STATIC folder (file not delivered)
        [  # Processed folder contains the file
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/TEST_STATIC_FILE_20240101.txt",
                    "name": "TEST_STATIC_FILE_20240101.txt",
                }
            )
        ],
        [  # Second call to processed folder for ls check
            FileInfoMock(
                {
                    "path": f"{test_container}/LRD_STATIC/processed/TEST_STATIC_FILE_20240101.txt",
                    "name": "TEST_STATIC_FILE_20240101.txt",
                }
            )
        ],
    ]
    mock_dbutils_fs_ls.side_effect = effect

    # Set deadline date
    deadline_date = datetime.now(timezone.utc) - timedelta(days=1) if deadline_passed else datetime.now(timezone.utc) + timedelta(days=1)
    
    # Call get_all_files with deadline information
    found_files = extraction.get_all_files(
        deadline_passed=deadline_passed,
        deadline_date=deadline_date
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
        assert found_files[0]["file_name"] == f"{test_container}/LRD_STATIC/TEST_STATIC_FILE.txt"
        # Check log message
        assert "Deadline has passed" in caplog.text
        assert "LRD_STATIC files copied from processed folder" in caplog.text
    else:
        # Should NOT copy the file
        mock_dbutils_fs_cp.assert_not_called()
        # No files should be found
        assert len(found_files) == 0
        # Check log messages
        assert "File TEST_STATIC_FILE not delivered but deadline not reached yet" in caplog.text
        assert "Deadline not yet reached" in caplog.text
        assert "LRD_STATIC files will not be copied" in caplog.text


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_place_static_data_keyword_only(
    spark_session,
    mocker,
    run_month,
    source_container,
):
    """Test that place_static_data requires deadline_passed as keyword argument."""
    # Create mock metadata DataFrame
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
                "LRD_STATIC",  # Uppercase
                "TEST_FILE",
                ".txt",
                "|",
                "test_file",
                0,
                "Expected",
            ),
        ],
        schema=schema_meta,
    )

    # Create schema for log DataFrame
    schema_log = StructType([
        StructField("SourceSystem", StringType(), True),
        StructField("SourceFileName", StringType(), True),
        StructField("DeliveryNumber", IntegerType(), True),
        StructField("FileDeliveryStep", IntegerType(), True),
        StructField("FileDeliveryStatus", StringType(), True),
        StructField("Result", StringType(), True),
        StructField("LastUpdatedDateTimestamp", TimestampType(), True),
        StructField("Comment", StringType(), True),
    ])
    
    # Create empty DataFrame with schema
    mock_log = spark_session.createDataFrame([], schema=schema_log)

    # Mock spark.read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, mock_log]

    # Create extraction instance
    extraction = ExtractNonSSFData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    # Mock the dbutils.fs.ls to prevent actual filesystem access
    mocker.patch.object(extraction.dbutils.fs, "ls", return_value=[])

    # Test that calling with positional argument raises TypeError
    with pytest.raises(TypeError, match="takes 2 positional arguments but 3 were given"):
        extraction.place_static_data([], True)  # noqa: FBT003 - Testing that positional bool fails

    # Test that calling with keyword argument works
    result = extraction.place_static_data([], deadline_passed=True)  # This should work
    assert isinstance(result, list)
