import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pyspark.sql import SparkSession

from src.config.exceptions import NonSSFExtractionError
from src.config.process import ProcessLogConfig
from src.staging.extract_nonssf_data import ExtractNonSSFData
from src.staging.status import NonSSFStepStatus
from src.utils.logging_util import get_logger
from src.utils.table_logging import write_to_log

logger = get_logger()

# Constants for magic numbers
MIN_ARGS_FOR_RUN_ID = 1
MIN_ARGS_FOR_DEADLINE = 2


def process_single_file(
    extraction: ExtractNonSSFData,
    file: dict[str, str],
    log_config: ProcessLogConfig,
) -> None:
    """Process a single file through all stages.
    
    Args:
        extraction: ExtractNonSSFData instance
        file: Dictionary with source_system and file_name
        log_config: Process log configuration
    """
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


def get_deadline_from_metadata(extraction: ExtractNonSSFData, source_system: str) -> datetime | None:
    """Get the earliest deadline from metadata for a given source system.
    
    Args:
        extraction: ExtractNonSSFData instance
        source_system: Source system to check
        
    Returns:
        Earliest deadline datetime or None if no deadlines found
    """
    deadlines = extraction.meta_data.filter(
        extraction.meta_data.SourceSystem == source_system
    ).select("Deadline").distinct().collect()
    
    earliest_deadline = None
    for row in deadlines:
        if row["Deadline"]:
            file_deadline = row["Deadline"]
            if isinstance(file_deadline, str):
                file_deadline_dt = datetime.strptime(file_deadline, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            else:
                file_deadline_dt = datetime.combine(file_deadline, datetime.min.time()).replace(tzinfo=timezone.utc)
            
            if earliest_deadline is None or file_deadline_dt < earliest_deadline:
                earliest_deadline = file_deadline_dt
    
    return earliest_deadline


def check_finob_nme_deadlines(extraction: ExtractNonSSFData, current_dt: datetime) -> dict[str, bool]:
    """Check which FINOB/NME files have passed their deadlines.
    
    Args:
        extraction: ExtractNonSSFData instance
        current_dt: Current datetime
        
    Returns:
        Dictionary of file paths that have passed their deadline
    """
    finob_nme_deadlines = {}
    for source in ["finob", "nme"]:
        deadlines = extraction.meta_data.filter(
            extraction.meta_data.SourceSystem == source
        ).select("SourceFileName", "Deadline").collect()
        
        for row in deadlines:
            if row["Deadline"]:
                file_deadline = row["Deadline"]
                if isinstance(file_deadline, str):
                    file_deadline_dt = datetime.strptime(file_deadline, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                else:
                    file_deadline_dt = datetime.combine(file_deadline, datetime.min.time()).replace(tzinfo=timezone.utc)
                
                # Check if this specific file's deadline has passed
                if current_dt >= file_deadline_dt:
                    finob_nme_deadlines[f"{source}/{row['SourceFileName']}"] = True
    
    return finob_nme_deadlines


def non_ssf_load(
    spark: SparkSession,
    run_month: str,
    run_id: int = 1,
    deadline_date: str | None = None,
) -> None:
    """Full load of Non-SSF data.

    1. Check availability of LRD_STATIC/NME/FINOB data in blob storage
    2. Copy processed LRD_STATIC for missing files (only after deadline and for
       expected files)
    3. Check deadline for FINOB and NME files and raise error if deadline passed
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
        deadline_date (str | None, optional): Deadline date in YYYY-MM-DD format. 
            If not provided, gets deadline from metadata table.

    Raises:
        NonSSFExtractionError: If any of the steps has status "Failed".
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
    
    # Get deadline from metadata if not provided
    current_dt = datetime.now(tz=timezone.utc)
    
    # Get the earliest deadline from metadata for LRD_STATIC files
    deadline_dt = get_deadline_from_metadata(extraction, "lrd_static")
    
    # Override with command line argument if provided
    if deadline_date:
        try:
            deadline_dt = datetime.strptime(deadline_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            logger.info(f"Using deadline date from command line: {deadline_dt}")
        except ValueError:
            logger.exception(
                f"Invalid deadline_date format: {deadline_date}. Expected YYYY-MM-DD"
            )
    
    deadline_passed = deadline_dt is not None and current_dt >= deadline_dt
    
    # Get all files from basel-nonssf-landing container and place static data
    # Pass deadline information to the extraction class
    files_per_delivery_entity = extraction.get_all_files(
        deadline_passed=deadline_passed,
        deadline_date=deadline_dt
    )
    
    if not files_per_delivery_entity:
        logger.error("No files found in basel-nonssf-landing container. ")
    else:
        logger.info(f"Processing {len(files_per_delivery_entity)} source files")

    logger.info(files_per_delivery_entity)

    # Check for deadline violations for FINOB and NME files
    finob_nme_deadlines = check_finob_nme_deadlines(extraction, current_dt)
    
    # If any FINOB/NME file has passed its deadline, check for violations
    if finob_nme_deadlines:
        extraction.check_deadline_violations(files_per_delivery_entity, log_config)

    # Process each file
    for file in files_per_delivery_entity:
        process_single_file(extraction, file, log_config)

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


if __name__ == "__main__":
    # Get args:
    if len(sys.argv) not in [1, 2, 3]:
        logger.error(
            "Incorrect number of parameters, expected 1, 2 or 3: "
            "run_month[ run_id][ deadline_date]"
        )
        sys.exit(-1)

    run_month, *remaining_args = sys.argv
    run_id = 1
    deadline_date = None
    
    if len(remaining_args) >= MIN_ARGS_FOR_RUN_ID:
        run_id = int(remaining_args[0])
    if len(remaining_args) >= MIN_ARGS_FOR_DEADLINE:
        deadline_date = remaining_args[1]
    
    non_ssf_load(spark, run_month, run_id, deadline_date)  # type: ignore[name-defined]
