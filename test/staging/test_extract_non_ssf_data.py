import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import call, patch

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from abnamro_bsrc_etl.staging.extract_nonssf_data import ExtractNonSSFData


class FileInfoMock(dict):
    """Allows to access dictionary keys as attributes.
    And adds a simple isDir method."""

    __getattr__ = dict.get

    def isDir(self):  # noqa: N802
        return bool(self.name.endswith("/") or self.name.endswith(".parquet"))


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202505", "test-container")],
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

    # Create deadline dates
    future_date = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
    past_date = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")

    # Create a mock DataFrame with deadline column
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
                "lrd_static",
                "TEST_NON_SSF_V1",
                ".txt",
                "|",
                "test_non_ssf_v1",
                0,
                "Expected",
                future_date,  # Future deadline
            ),
            (
                "lrd_static",
                "TEST_NON_SSF_V2",
                ".txt",
                "|",
                "test_non_ssf_v2",
                0,
                "Expected",
                past_date,  # Past deadline - should be copied
            ),
            (
                "nme",
                "TEST_NON_SSF_V3",
                ".parquet",
                ",",
                "test_non_ssf_v3",
                0,
                "Expected",
                future_date,
            ),
            (
                "finob",
                "TEST_NON_SSF_V4",
                ".csv",
                ",",
                "test_non_ssf_v4",
                0,
                "Expected",
                past_date,  # Past deadline
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
                "lrd_static",
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

    # Initialize process log
    extraction.initialize_process_log(run_id=1)
    assert extraction.base_process_record["RunID"] == 1
    assert extraction.base_process_record["Component"] == "Non-SSF"

    # Verify that spark.read.table was called with the correct arguments
    mock_read.table.assert_any_call(f"bsrc_d.metadata_{run_month}.metadata_nonssf")
    mock_read.table.assert_any_call(f"bsrc_d.log_{run_month}.log_nonssf")

    # Test deadline checking
    deadline_reached, deadline_str = extraction.check_deadline_reached(
        "TEST_NON_SSF_V1"
    )
    assert not deadline_reached  # Future deadline
    assert deadline_str == future_date

    deadline_reached, deadline_str = extraction.check_deadline_reached(
        "TEST_NON_SSF_V2"
    )
    assert deadline_reached  # Past deadline
    assert deadline_str == past_date

    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")
    effect = [
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
                ("TEST_NON_SSF_V3.csv", "NME"),
                ("processed/", "NME"),
            ],
            [("TEST_NON_SSF_V4.csv", "FINOB"), ("processed/", "FINOB")],
            [
                ("TEST_NON_SSF_V1.txt", "LRD_STATIC"),
                ("TEST_NON_SSF_V5.txt", "LRD_STATIC"),
                ("processed/", "LRD_STATIC"),
            ],
            [("TEST_NON_SSF_V2_999999.txt", "LRD_STATIC/processed")],
            [("TEST_NON_SSF_V2_999999.txt", "LRD_STATIC/processed")],
        ]
    ]
    mock_dbutils_fs_ls.side_effect = effect
    mock_dbutils_fs_cp = mocker.patch.object(extraction.dbutils.fs, "cp")
    mock_dbutils_fs_mv = mocker.patch.object(extraction.dbutils.fs, "mv")

    found_files = extraction.get_all_files()

    # V2 should be copied because deadline is reached
    mock_dbutils_fs_cp.assert_any_call(
        f"{test_container}/LRD_STATIC/processed/TEST_NON_SSF_V2_999999.txt",
        f"{test_container}/LRD_STATIC/TEST_NON_SSF_V2.txt",
    )

    # V1 should not be copied because deadline is not reached
    assert "Deadline not reached for TEST_NON_SSF_V1" in caplog.text

    assert (
        "File TEST_NON_SSF_V5 not found in metadata. "
        "Please check if it should be delivered." in caplog.messages
    )

    # Test check_missing_files_after_deadline
    # Mock the ls calls for this test
    with patch.object(extraction.dbutils.fs, "ls") as mock_ls_missing:
        # Set up the mock to simulate missing FINOB file after deadline
        def ls_side_effect(path):
            if "FINOB" in path:
                return []  # No files in FINOB
            if "NME" in path:
                return [
                    FileInfoMock(
                        {"name": "TEST_NON_SSF_V3.parquet", "isDir": lambda: False}
                    )
                ]
            if "LRD_STATIC/processed" in path:
                return [
                    FileInfoMock(
                        {"name": "TEST_NON_SSF_V2_999999.txt", "isDir": lambda: False}
                    )
                ]
            if "LRD_STATIC" in path:
                return [
                    FileInfoMock(
                        {"name": "TEST_NON_SSF_V1.txt", "isDir": lambda: False}
                    )
                ]
            return []

        mock_ls_missing.side_effect = ls_side_effect

        missing_files = extraction.check_missing_files_after_deadline()

        # V4 should be missing (FINOB, past deadline)
        assert any(f["file_name"] == "TEST_NON_SSF_V4" for f in missing_files)
        # V3 should not be missing (NME, future deadline)
        assert not any(f["file_name"] == "TEST_NON_SSF_V3" for f in missing_files)

    # Test log_missing_files_errors
    missing_test_files = [
        {"source_system": "FINOB", "file_name": "TEST_FILE", "deadline": "2024-01-01"}
    ]
    has_critical = extraction.log_missing_files_errors(missing_test_files)
    assert has_critical  # FINOB is critical

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
    # + 1 step for missing file error log
    # So in total 27 calls for metadata and log
    assert metadata_path_calls == 27
    assert log_path_calls == 27


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202505", "test-container")],
)
def test_deadline_functionality(
    spark_session,
    mocker,
    run_month,
    source_container,
    caplog,
):
    """Test specific deadline functionality."""
    # Create deadline dates
    future_date = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
    past_date = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")

    # Create mock metadata with various deadline scenarios
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
            # LRD_STATIC with past deadline - should be copied
            (
                "lrd_static",
                "STATIC_PAST",
                ".txt",
                "|",
                "static_past",
                0,
                "Expected",
                past_date,
            ),
            # LRD_STATIC with future deadline - should NOT be copied
            (
                "lrd_static",
                "STATIC_FUTURE",
                ".txt",
                "|",
                "static_future",
                0,
                "Expected",
                future_date,
            ),
            # LRD_STATIC with no deadline - should NOT be copied
            (
                "lrd_static",
                "STATIC_NO_DEADLINE",
                ".txt",
                "|",
                "static_no_deadline",
                0,
                "Expected",
                None,
            ),
        ],
        schema=schema_meta,
    )

    # Create mock log with proper schema
    log_schema = [
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
                "lrd_static",
                "DUMMY",
                1,
                0,
                "Expected",
                "Success",
                datetime.now(timezone.utc),
                "Test comment",
            )
        ],
        schema=log_schema,
    )

    # Mock spark read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, mock_log]

    # Create extraction instance
    extraction = ExtractNonSSFData(
        spark_session, run_month, source_container=source_container
    )
    extraction.initialize_process_log()

    # Mock dbutils
    mock_dbutils_fs_ls = mocker.patch.object(extraction.dbutils.fs, "ls")
    mock_dbutils_fs_cp = mocker.patch.object(extraction.dbutils.fs, "cp")

    # Set up ls to show files in processed folder
    mock_dbutils_fs_ls.side_effect = [
        [
            FileInfoMock({"name": "STATIC_PAST_20240101.txt", "path": "path1"}),
            FileInfoMock({"name": "STATIC_FUTURE_20240101.txt", "path": "path2"}),
            FileInfoMock({"name": "STATIC_NO_DEADLINE_20240101.txt", "path": "path3"}),
        ],
        ["dummy"],  # For the second ls call
        ["dummy"],  # For the third ls call
        ["dummy"],  # For the fourth ls call
    ]

    # Test place_static_data
    new_files = []
    extraction.place_static_data(new_files)

    # Only STATIC_PAST should be copied (deadline reached)
    mock_dbutils_fs_cp.assert_called_once()
    assert any(
        "STATIC_PAST.txt" in str(call) for call in mock_dbutils_fs_cp.call_args_list
    )
    assert not any(
        "STATIC_FUTURE.txt" in str(call) for call in mock_dbutils_fs_cp.call_args_list
    )
    assert not any(
        "STATIC_NO_DEADLINE.txt" in str(call)
        for call in mock_dbutils_fs_cp.call_args_list
    )

    # Check that deadline not reached message appears for STATIC_FUTURE
    assert "Deadline not reached for STATIC_FUTURE" in caplog.text


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202505", "test-container")],
)
def test_append_to_process_log(
    spark_session,
    mocker,
    run_month,
    source_container,
):
    """Test the append_to_process_log method."""
    # Mock metadata and log
    mock_meta = spark_session.createDataFrame([("dummy", 1)], schema=["col1", "col2"])
    mock_log = spark_session.createDataFrame([("dummy", 1)], schema=["col1", "col2"])

    # Mock spark read
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.table.side_effect = [mock_meta, mock_log]

    # Mock write_to_log
    mock_write_to_log = mocker.patch(
        "abnamro_bsrc_etl.utils.table_logging.write_to_log"
    )

    # Create extraction instance
    extraction = ExtractNonSSFData(
        spark_session, run_month, source_container=source_container
    )
    extraction.initialize_process_log(run_id=123)

    # Test append_to_process_log
    extraction.append_to_process_log(
        source_system="TEST_SYSTEM", comments="Test comment", status="Started"
    )

    # Verify write_to_log was called
    mock_write_to_log.assert_called_once()
    call_args = mock_write_to_log.call_args

    assert call_args[1]["spark"] == spark_session
    assert call_args[1]["run_month"] == run_month
    assert call_args[1]["log_table"] == "process_log"

    record = call_args[1]["record"]
    assert record["RunID"] == 123
    assert record["Status"] == "Started"
    assert record["Comments"] == "Test comment"
    assert record["SourceSystem"] == "TEST_SYSTEM"
    assert record["Component"] == "Non-SSF"
