from datetime import datetime, timezone
from unittest.mock import call

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.staging.extract_dial_data import ExtractDialData


@pytest.mark.parametrize(
    ("run_month", "source_container"),
    [("202503", "test-container")],
)
def test_extract_dial_data(
    spark_session,
    mocker,
    run_month,
    source_container,
):
    test_container = f"abfss://{source_container}@bsrcdadls.dfs.core.windows.net"
    month_container = f"abfss://{run_month}@bsrcdadls.dfs.core.windows.net"
    metadata_path = f"bsrc_d.metadata_{run_month}.metadata_dial_schedule"
    log_path = f"bsrc_d.log_{run_month}.log_dial_schedule"

    # Create a mock DataFrame
    schema_trigger = [
        "dial_consumer_name",
        "dial_source_system",
        "dial_source_file_name",
        "dial_source_file_snapshot_datetime",
        "dial_notification_datetime",
        "dial_target_file_full_path",
        "dial_target_file_row_count",
    ]
    mock_trigger = spark_session.createDataFrame(
        [
            ("src", "epr", "TEST_DIAL_V2", "202503", "9999", "path/to/dest2/", 102),
            ("src", "epr", "TEST_DIAL_V3", "202503", "9999", "path/to/dest3/", 103),
            ("src", "epr", "TEST_DIAL_V4", "202509", "9999", "path/to/dest4/", 104),
            ("src", "epr", "TEST_DIAL_V5", "202509", "9999", "path/to/dest5/", 105),
        ],
        schema=schema_trigger,
    )

    schema_meta = [
        "FileDeliveryStatus",
        "FileDeliveryStep",
        "SourceFileName",
        "SnapshotDate",
        "StgTableName",
    ]
    mock_meta = spark_session.createDataFrame(
        [
            ("Extracted", 1, "TEST_DIAL_V1", "202503", "test_dial_v1"),
            ("Expected delivery", 0, "TEST_DIAL_V2", "202503", "test_dial_v2"),
            ("Expected delivery", 0, "TEST_DIAL_V3", "202503", "test_dial_v3"),
        ],
        schema=schema_meta,
    )
    schema_log = [
        "SourceSystem",
        "SourceFileName",
        "SnapshotDate",
        "FileDeliveryStatus",
        "FileDeliveryStep",
        "DeliveryNumber",
        "LastUpdatedDateTimestamp",
        "Comment",
    ]
    mock_log = spark_session.createDataFrame(
        [
            (
                "epr",
                "TEST_DIAL_V2",
                "202503",
                "Expected delivery",
                0,
                0,
                datetime.now(tz=timezone.utc),
                "Data file stored in storage location.",
            ),
            (
                "epr",
                "TEST_DIAL_V3",
                "202503",
                "Expected delivery",
                0,
                0,
                datetime.now(tz=timezone.utc),
                "Data file stored in storage location.",
            ),
        ],
        schema=schema_log,
    )

    schema_staging_v2 = ["int_col", "str_col"]
    mock_staging_v2 = spark_session.createDataFrame(
        data=[(3, "b")], schema=schema_staging_v2
    )

    schema_staging_v3 = ["dt_col", "bool_col"]
    mock_staging_v3 = spark_session.createDataFrame(
        data=[(datetime.now(tz=timezone.utc), True)], schema=schema_staging_v3
    )
    mock_data_path_2 = spark_session.createDataFrame(
        [(x,) for x in range(102)],
        schema=StructType([StructField("col02", IntegerType())]),
    )
    mock_data_path_3 = spark_session.createDataFrame(
        [(x,) for x in range(103)],
        schema=StructType([StructField("col03", IntegerType())]),
    )

    # Mock spark.read.json and spark.read.table to return the mock DataFrames
    mock_read = mocker.patch("pyspark.sql.SparkSession.read", autospec=True)
    mock_read.json.return_value = mock_trigger
    mock_read.table.side_effect = [
        mock_meta,
        mock_log,
        mock_staging_v2,
        mock_staging_v3,
    ]

    # Check ExtractDialData class initialisation
    extraction = ExtractDialData(
        spark_session,
        run_month,
        source_container=source_container,
    )

    mock_read.json.assert_called_once_with(
        f"{test_container}/dial_trigger",
        schema=StructType(
            [
                StructField("dial_source_file_name", StringType()),
                StructField("dial_source_system", StringType()),
                StructField("dial_source_file_snapshot_datetime", StringType()),
                StructField("dial_notification_datetime", StringType()),
                StructField("dial_consumer_name", StringType()),
                StructField("dial_target_file_full_path", StringType()),
                StructField("dial_target_file_row_count", StringType()),
            ]
        ),
    )
    # Verify that spark.read.table was called with the correct arguments
    mock_read.table.assert_any_call("bsrc_d.metadata_202503.metadata_dial_schedule")
    mock_read.table.assert_any_call("bsrc_d.log_202503.log_dial_schedule")

    assert extraction.trigger_data == mock_trigger

    matching_meta_trigger_data = extraction.get_matching_meta_trigger_data()
    triggers, triggers_archive = (
        extraction.get_triggers(matching_meta_trigger_data, matching=matching)
        for matching in [True, False]
    )
    assert matching_meta_trigger_data.columns == [
        *schema_trigger,
        *schema_meta,
        "trigger_file_name",
    ]
    assert matching_meta_trigger_data.filter("SourceFileName is not null").count() == 2
    assert matching_meta_trigger_data.filter("SourceFileName is null").count() == 2

    # Create a mock dbutils
    mock_dbutils = mocker.patch.object(
        extraction, "dbutils", new_callable=mocker.MagicMock()
    )

    # Mock for reading parquet files
    mock_read.parquet.side_effect = [mock_data_path_2, mock_data_path_3]

    # Mock for writing parquet files
    mock_write = mocker.MagicMock()
    mock_mode = mocker.MagicMock()
    mock_write.mode.return_value = mock_mode
    mock_mode.parquet.return_value = None
    mocker.patch.object(
        DataFrame,
        "write",
        new_callable=mocker.PropertyMock,
        return_value=mock_write,
    )

    # Run full flow
    for trigger_file_name, trigger_config in triggers.items():
        file_mapping = extraction.get_file_mapping(
            matching_meta_trigger_data.filter("SourceFileName is not null")
        )[trigger_config["file_name"]]
        data_path, row_count, stg_table_name = tuple(file_mapping.values())

        # 1. Extracted raw parquet data
        data = extraction.extract_from_blob_storage(
            data_path=data_path,
            **trigger_config,
        )

        # 2. Validated expected count
        result_row_count = extraction.validate_expected_row_count(
            data=data,
            expected_row_count=row_count,
            **trigger_config,
        )

        # 2. Validated expected count - Unhappy
        result_row_count_unhappy = extraction.validate_expected_row_count(
            data=data,
            expected_row_count=+1,
            **trigger_config,
        )

        # 3. Copy parquet file
        result_copy_to_blob = extraction.copy_to_blob_storage(
            data=data,
            **trigger_config,
        )

        # 4. Moved JSON trigger file
        extraction.move_trigger_file(
            trigger_file_name=trigger_file_name,
            **trigger_config,
        )

        # 5. Loaded Staging table
        result_staging = extraction.save_to_stg_table(
            data=data,
            stg_table_name=stg_table_name,
            **trigger_config,
        )

        # 6. Checked Data Quality
        result_data_quality = extraction.validate_data_quality(
            stg_table_name=stg_table_name,
            **trigger_config,
            dq_check_folder="test/data",
        )

    # 7. Archive matching trigger file
    for trigger_file_name in triggers_archive:
        extraction.move_archive_file(trigger_file_name)

    # Check `1 - Extracted raw parquet data`
    mock_read.parquet.assert_any_call("path/to/dest2/")
    mock_read.parquet.assert_any_call("path/to/dest3/")

    # Check `2 - Validated expected count`
    assert result_row_count is True
    assert result_row_count_unhappy is False

    # Check `3. Copied parquet to storage account`
    mock_write.parquet.assert_any_call(
        f"{month_container}/sourcing_landing_data/DIAL/epr/TEST_DIAL_V2.parquet",
        mode="overwrite",
    )
    mock_write.parquet.assert_any_call(
        f"{month_container}/sourcing_landing_data/DIAL/epr/TEST_DIAL_V3.parquet",
        mode="overwrite",
    )
    assert result_copy_to_blob is True

    # Check `4. Moved JSON trigger file` (trigger + archive)
    mock_dbutils.fs.mv.assert_any_call(
        f"{test_container}/dial_trigger/src_epr_TEST_DIAL_V2_202503_9999.json",
        f"{month_container}/sourcing_landing_data/DIAL/epr_trigger/src_epr_TEST_DIAL_V2_202503_9999.json",
    )
    mock_dbutils.fs.mv.assert_any_call(
        f"{test_container}/dial_trigger/src_epr_TEST_DIAL_V3_202503_9999.json",
        f"{month_container}/sourcing_landing_data/DIAL/epr_trigger/src_epr_TEST_DIAL_V3_202503_9999.json",
    )
    mock_dbutils.fs.mv.assert_any_call(
        f"{test_container}/dial_trigger/src_epr_TEST_DIAL_V4_202509_9999.json",
        f"{test_container}/dial_trigger_archive/src_epr_TEST_DIAL_V4_202509_9999.json",
    )
    mock_dbutils.fs.mv.assert_any_call(
        f"{test_container}/dial_trigger/src_epr_TEST_DIAL_V5_202509_9999.json",
        f"{test_container}/dial_trigger_archive/src_epr_TEST_DIAL_V5_202509_9999.json",
    )

    # Check `5. Loaded Staging table`
    mock_write.mode().saveAsTable.assert_any_call("bsrc_d.stg_202503.test_dial_v2")
    mock_write.mode().saveAsTable.assert_any_call("bsrc_d.stg_202503.test_dial_v3")
    assert result_staging is True

    # Check `6. Checked Data Quality`
    assert result_data_quality is True

    # Count update metadata and log calls
    metadata_path_calls, log_path_calls = (
        len(
            [
                call_args
                for call_args in mock_write.mode(mode).saveAsTable.call_args_list
                if call_args == call(path)
            ]
        )
        for path, mode in zip(
            [metadata_path, log_path], ["overwrite", "append"], strict=False
        )
    )

    # For every matching data (v2 + v3) we log every step from 2-7: (2 x 6)
    # For the count validation (step 2) we also have the unhappy flow (2 x 1)
    # And for every non-matching trigger (v4 + v5) we do not log anything (2 x 0)
    # So in total 12 + 2 = 14
    assert metadata_path_calls == 14
    assert log_path_calls == 14
