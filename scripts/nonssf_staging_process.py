from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pyspark.sql import SparkSession

from abnamro_bsrc_etl.config.exceptions import NonSSFExtractionError
from abnamro_bsrc_etl.config.process import ProcessLogConfig
from abnamro_bsrc_etl.staging.extract_nonssf_data import ExtractNonSSFData
from abnamro_bsrc_etl.staging.status import NonSSFStepStatus
from abnamro_bsrc_etl.utils.logging_util import get_logger
from abnamro_bsrc_etl.utils.table_logging import write_to_log

logger = get_logger()


def non_ssf_load(
    spark: SparkSession,
    run_month: str,
    run_id: int = 1,
) -> None:
    """Full load of Non-SSF data.

    1. Check availability of LRD_STATIC/NME/FINOB data in blob storage
    2. Copy processed LRD_STATIC for missing files (only after deadline)
    3. Check for missing files after deadline and fail if any
    4. For every file in blob storage:
        1. Initial checks
        2. Convert to parquet and copy to month_no/sourcing_landing_data/NON_SSF/<>
        3. Move source file to processed folder
        4. Lookup table name in metadata table and load to staging
        5. Run DQ checks on staging table

    All steps are logged in process log.

    Args:
        spark (SparkSession): Spark session
        run_month (str): Run month in yyyymm format
        run_id (int, optional): Run ID. Defaults to 1.

    Raises:
        NonSSFExtractionError: If any of the steps has status "Failed" or if
            files are missing after their deadline.
    """
    base_record: dict[str, int | datetime | str] = {
        "RunID": run_id,
        "Timestamp": datetime.now(tz=timezone.utc),
        "Workflow": "Staging",
        "Component": "Non-SSF",
        "Layer": "Staging",
    }

    log_config: ProcessLogConfig = {
        "spark": spark,
        "run_month": run_month,
        "record": base_record,
    }

    # Start the process
    append_to_process_log(**log_config, comments="", source_system="", status="Started")

    extraction = ExtractNonSSFData(spark, run_month=run_month)
    
    # Check for missing files after deadline BEFORE processing
    missing_files = extraction.check_missing_files_after_deadline()
    if missing_files:
        # Log errors for missing files
        has_critical_missing = extraction.log_missing_files_errors(missing_files)
        
        # Only log the error but don't fail the process immediately
        # This allows the process to continue and process available files
        if has_critical_missing:
            nme_finob_missing = [
                f for f in missing_files 
                if f['source_system'].upper() in ['NME', 'FINOB']
            ]
            error_summary = (
                f"Critical files missing after deadline: "
                f"{', '.join([f['file_name'] for f in nme_finob_missing])}"
            )
            logger.error(error_summary)
            # Don't raise exception here - let the process continue
    
    # Get all files from basel-nonssf-landing container and place static data
    files_per_delivery_entity = extraction.get_all_files()
    if not files_per_delivery_entity:
        logger.error("No files found in basel-nonssf-landing container. ")
    else:
        logger.info(f"Processing {len(files_per_delivery_entity)} source files")

    logger.info(files_per_delivery_entity)

    for file in files_per_delivery_entity:
        source_system = file["source_system"]
        file_name = file["file_name"]
        file_comment = f"Processing {Path(file_name).stem}"
        # Start the process for corresponding trigger file
        append_to_process_log(
            **log_config,
            source_system=source_system,
            status="Started",
            comments=file_comment,
        )

        # 1. Initial checks
        if not extraction.initial_checks(
            file_name=file_name, source_system=source_system
        ):
            append_to_process_log(
                **log_config,
                source_system=source_system,
                file_delivery_status=NonSSFStepStatus.INIT_CHECKS,
                comments=file_comment,
                status="Failed",
            )

        # 2. Convert to parquet and place in month container
        if not extraction.convert_to_parquet(
            source_system=source_system,
            file_name=file_name,
        ):
            append_to_process_log(
                **log_config,
                source_system=source_system,
                file_delivery_status=NonSSFStepStatus.CONVERTED_PARQUET,
                comments=file_comment,
                status="Failed",
            )

        # 3. Move source file to processed folder
        if not extraction.move_source_file(
            source_system=source_system, file_name=file_name
        ):
            append_to_process_log(
                **log_config,
                source_system=source_system,
                file_delivery_status=NonSSFStepStatus.MOVED_SRC,
                comments=file_comment,
                status="Failed",
            )

        # 4. Load to staging table
        data = extraction.extract_from_parquet(
            source_system=source_system, file_name=file_name
        )
        stg_table_name = extraction.get_staging_table_name(file_name)
        if not extraction.save_to_stg_table(
            data=data,
            stg_table_name=stg_table_name,
            source_system=source_system,
            file_name=Path(file_name).stem,
        ):
            append_to_process_log(
                **log_config,
                source_system=source_system,
                file_delivery_status=NonSSFStepStatus.LOADED_STG,
                comments=file_comment,
                status="Failed",
            )

        # 5. DQ checks
        elif not extraction.validate_data_quality(
            source_system=source_system,
            file_name=Path(file_name).stem,
            stg_table_name=stg_table_name,
        ):
            append_to_process_log(
                **log_config,
                source_system=source_system,
                file_delivery_status=NonSSFStepStatus.CHECKED_DQ,
                comments=file_comment,
                status="Failed",
            )

        # Complete the process for corresponding trigger file
        extraction.update_log_metadata(
            source_system=source_system,
            key=Path(file_name).stem,
            file_delivery_status=NonSSFStepStatus.COMPLETED,
            result="SUCCESS",
            comment=file_comment,
        )
        append_to_process_log(
            **log_config,
            source_system=source_system,
            comments=file_comment,
            status="Completed",
        )

    # Complete the process after all trigger files
    append_to_process_log(
        **log_config, comments="", source_system="", status="Completed"
    )


def append_to_process_log(
    spark: SparkSession,
    run_month: str,
    record: dict[str, int | datetime | str],
    source_system: str,
    comments: str,
    status: Literal["Completed", "Started", "Failed"] = "Completed",
    file_delivery_status: NonSSFStepStatus = NonSSFStepStatus.COMPLETED,
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
        NonSSFExtractionError: If status is "Failed".
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
        raise NonSSFExtractionError(file_delivery_status, additional_info=comments)
