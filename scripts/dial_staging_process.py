import sys
from pathlib import Path

sys.path.append(f"{Path.cwd().parent.resolve()}")

from datetime import datetime, timezone
from typing import Literal

from pyspark.sql import SparkSession

from src.config.exceptions import DialExtractionError
from src.config.process import ProcessLogConfig
from src.staging.extract_dial_data import ExtractDialData
from src.staging.status import DialStepStatus
from src.utils.logging_util import get_logger
from src.utils.table_logging import write_to_log

logger = get_logger()


def dial_load(  # noqa: C901
    spark: SparkSession,
    run_month: str,
    run_id: int = 1,
) -> None:
    """Full load of DIAL data.

    First the trigger files are joined with the metadata table to get the
    source system and file name. Then the following steps are performed:

    1. Extract data from Azure Blob Storage
    2. Checks record count to validate completeness
    3. Copy parquet file to blob storage
    4. Move matching trigger file to azure blob storage
    5. Load data into staging schema
    6. Data quality checks
    7. Move archive trigger file to archive folder

    All steps are logged in process log.

    Args:
        spark (SparkSession): Spark session
        run_month (str): Run month in yyyymm format
        run_id (int, optional): Run ID. Defaults to 1.

    Raises:
        DialExtractionError: If any of the steps has status "Failed".
    """
    base_record: dict[str, int | datetime | str] = {
        "RunID": run_id,
        "Timestamp": datetime.now(tz=timezone.utc),
        "Workflow": "Staging",
        "Component": "DIAL",
        "Layer": "Staging",
    }

    log_config_trigger: ProcessLogConfig = {
        "spark": spark,
        "run_month": run_month,
        "record": base_record,
    }

    # Start the process
    append_to_process_log(
        **log_config_trigger, comments="", source_system="", status="Started"
    )

    extraction = ExtractDialData(spark, run_month=run_month)
    matching_meta_trigger_data = extraction.get_matching_meta_trigger_data()
    triggers, triggers_archive = (
        extraction.get_triggers(matching_meta_trigger_data, matching=matching)
        for matching in [True, False]
    )
    if not triggers:
        logger.info(
            "No matches on SourceFileName and SnapshotDate for run_month: "
            f"{run_month}"
        )
    else:
        logger.info(f"Processing {len(triggers)} trigger files")
        file_mappings = extraction.get_file_mapping(
            matching_meta_trigger_data.filter("SourceFileName is not null")
        )

    for trigger_file_name, trigger_config in triggers.items():
        source_system, file_name, snapshot_date = (
            trigger_config[k] for k in ["source_system", "file_name", "snapshot_date"]
        )
        log_trigger_info(source_system, file_name, snapshot_date, "trigger")
        trigger_comment = f"SourceFileName: {file_name}"

        # Start the process for corresponding trigger file
        append_to_process_log(
            **log_config_trigger,
            source_system=source_system,
            comments=trigger_comment,
            status="Started",
        )

        file_mapping = file_mappings[file_name]
        data_path, row_count, stg_table_name = (
            file_mapping[k] for k in ["data_path", "row_count", "stg_table_name"]
        )

        # 1. Extract Data
        dial_data = extraction.extract_from_blob_storage(
            data_path=data_path,
            **trigger_config,
        )
        if not dial_data:
            append_to_process_log(
                **log_config_trigger,
                source_system=source_system,
                file_delivery_status=DialStepStatus.EXTRACTED,
                comments=trigger_comment,
                status="Failed",
            )

        # 2. Completeness checks
        elif not extraction.validate_expected_row_count(
            data=dial_data,
            expected_row_count=row_count,
            **trigger_config,
        ):
            append_to_process_log(
                **log_config_trigger,
                source_system=source_system,
                file_delivery_status=DialStepStatus.VALIDATED_ROW_COUNT,
                comments=trigger_comment,
                status="Failed",
            )

        # 3. Copy parquet file
        elif not extraction.copy_to_blob_storage(
            data=dial_data,
            **trigger_config,
        ):
            append_to_process_log(
                **log_config_trigger,
                source_system=source_system,
                file_delivery_status=DialStepStatus.COPIED_PARQUET,
                comments=trigger_comment,
                status="Failed",
            )

        # 4. Move matching trigger file
        elif not extraction.move_trigger_file(
            trigger_file_name=trigger_file_name,
            **trigger_config,
        ):
            append_to_process_log(
                **log_config_trigger,
                source_system=source_system,
                file_delivery_status=DialStepStatus.MOVED_JSON,
                comments=trigger_comment,
                status="Failed",
            )

        # 5. Load into STG schema
        elif not extraction.save_to_stg_table(
            data=dial_data,
            stg_table_name=stg_table_name,
            **trigger_config,
        ):
            append_to_process_log(
                **log_config_trigger,
                source_system=source_system,
                file_delivery_status=DialStepStatus.LOADED_STG,
                comments=trigger_comment,
                status="Failed",
            )

        # 6. DQ checks
        elif not extraction.validate_data_quality(
            stg_table_name=stg_table_name,
            **trigger_config,
        ):
            append_to_process_log(
                **log_config_trigger,
                source_system=source_system,
                file_delivery_status=DialStepStatus.CHECKED_DQ,
                comments=trigger_comment,
                status="Failed",
            )

        # Complete the process for corresponding trigger file
        extraction.update_log_metadata(
            source_system=source_system,
            key=file_name,
            snapshot_date=snapshot_date,
            file_delivery_status=DialStepStatus.COMPLETED,
            result="SUCCESS",
            comment=trigger_comment,
        )
        append_to_process_log(
            **log_config_trigger,
            source_system=source_system,
            comments=trigger_comment,
            status="Completed",
        )
    # 7. Archive matching trigger file
    logger.info(f"Archiving {len(triggers_archive)} files")
    for file_name_archive, archive_config in triggers_archive.items():
        source_system, file_name, snapshot_date = (
            archive_config[k] for k in ["source_system", "file_name", "snapshot_date"]
        )
        log_trigger_info(source_system, file_name, snapshot_date, "archive")
        archive_comment = f"Archive: {file_name}"
        if not extraction.move_archive_file(file_name_archive):
            raise DialExtractionError(
                DialStepStatus.MOVED_JSON, additional_info=archive_comment
            )

    # Complete the process after all trigger files
    append_to_process_log(
        **log_config_trigger, comments="", source_system="", status="Completed"
    )


def append_to_process_log(
    spark: SparkSession,
    run_month: str,
    record: dict[str, int | datetime | str],
    source_system: str,
    comments: str,
    status: Literal["Completed", "Started", "Failed"] = "Completed",
    file_delivery_status: DialStepStatus = DialStepStatus.COMPLETED,
) -> None:
    """Append log entry to process log table.

    Args:
        spark (SparkSession): SparkSession
        run_month (str): Run month ID
        record (RecordConfig): Data record, incl all columns of process log table.
        source_system (str): Source System
        comment (str): Comment of step
        status (Literal["Completed", "Started", "Failed"]): Status of the step.
            Defaults to "Completed".

    Raises:
        DialExtractionError: If status is "Failed".
    """
    record["Status"] = status
    record["Comments"] = comments
    record["SourceSystem"] = source_system
    write_to_log(
        spark=spark,
        run_month=run_month,
        record=dict(record),
        log_table="process_log",
    )
    if status == "Failed":
        # Overall process should be set to failed as well
        record["SourceSystem"] = ""
        write_to_log(
            spark=spark,
            run_month=run_month,
            record=dict(record),
            log_table="process_log",
        )
        raise DialExtractionError(file_delivery_status, additional_info=comments)


def log_trigger_info(
    source_system: str,
    file_name: str,
    snapshot_date: str,
    file_type: Literal["trigger", "archive"],
) -> None:
    return logger.info(
        f"Processing {file_type} file\n%s",
        "\n".join(
            [
                f"SourceSystem: {source_system}",
                f"SourceFileName: {file_name}",
                f"SnapshotDate: {snapshot_date}",
            ]
        ),
    )


if __name__ == "__main__":
    # Get args:
    if len(sys.argv) not in [2, 3]:  # First argument is script name
        logger.error(
            "Incorrect number of parameters, expected 1 or 2: run_month[ run_id]"
        )
        sys.exit(-1)

    script, run_month, *run_id_list = sys.argv
    run_id = 1 if not run_id_list else int(run_id_list[0])
    dial_load(spark, run_month, run_id)  # type: ignore[name-defined]
