from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from abnamro_bsrc_etl.config.exceptions import NonSSFExtractionError
from abnamro_bsrc_etl.staging.extract_nonssf_data import ExtractNonSSFData
from abnamro_bsrc_etl.staging.status import NonSSFStepStatus
from abnamro_bsrc_etl.utils.logging_util import get_logger

logger = get_logger()


def _handle_extraction_error(
    extraction: ExtractNonSSFData,
    source_system: str,
    file_delivery_status: NonSSFStepStatus,
    file_comment: str,
) -> None:
    """Handle extraction errors by logging and raising exception.
    
    Args:
        extraction: ExtractNonSSFData instance
        source_system: Source system name
        file_delivery_status: Current step status
        file_comment: Comment for logging
        
    Raises:
        NonSSFExtractionError: Always raises with the provided status and comment
    """
    extraction.append_to_process_log(
        source_system=source_system,
        comments=file_comment,
        status="Failed",
    )
    raise NonSSFExtractionError(file_delivery_status, additional_info=file_comment)


def _process_single_file(
    extraction: ExtractNonSSFData,
    file: dict[str, str],
) -> None:
    """Process a single file through all stages.
    
    Args:
        extraction: ExtractNonSSFData instance
        file: Dictionary with 'source_system' and 'file_name' keys
        
    Raises:
        NonSSFExtractionError: If any processing step fails
    """
    source_system = file["source_system"]
    file_name = file["file_name"]
    file_comment = f"Processing {Path(file_name).stem}"
    
    # Start the process for corresponding trigger file
    extraction.append_to_process_log(
        source_system=source_system,
        status="Started",
        comments=file_comment,
    )

    # 1. Initial checks
    if not extraction.initial_checks(
        file_name=file_name, source_system=source_system
    ):
        _handle_extraction_error(
            extraction, source_system, NonSSFStepStatus.INIT_CHECKS, file_comment
        )

    # 2. Convert to parquet and place in month container
    if not extraction.convert_to_parquet(
        source_system=source_system,
        file_name=file_name,
    ):
        _handle_extraction_error(
            extraction, source_system, NonSSFStepStatus.CONVERTED_PARQUET, file_comment
        )

    # 3. Move source file to processed folder
    if not extraction.move_source_file(
        source_system=source_system, file_name=file_name
    ):
        _handle_extraction_error(
            extraction, source_system, NonSSFStepStatus.MOVED_SRC, file_comment
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
        _handle_extraction_error(
            extraction, source_system, NonSSFStepStatus.LOADED_STG, file_comment
        )

    # 5. DQ checks
    elif not extraction.validate_data_quality(
        source_system=source_system,
        file_name=Path(file_name).stem,
        stg_table_name=stg_table_name,
    ):
        _handle_extraction_error(
            extraction, source_system, NonSSFStepStatus.CHECKED_DQ, file_comment
        )

    # Complete the process for corresponding trigger file
    extraction.update_log_metadata(
        source_system=source_system,
        key=Path(file_name).stem,
        file_delivery_status=NonSSFStepStatus.COMPLETED,
        result="SUCCESS",
        comment=file_comment,
    )
    extraction.append_to_process_log(
        source_system=source_system,
        comments=file_comment,
        status="Completed",
    )


def _check_and_handle_missing_files(extraction: ExtractNonSSFData) -> None:
    """Check for missing files after deadline and handle critical ones.
    
    Args:
        extraction: ExtractNonSSFData instance
        
    Raises:
        NonSSFExtractionError: If critical files (NME/FINOB) are missing after deadline
    """
    missing_files = extraction.check_missing_files_after_deadline()
    if missing_files:
        # Log errors for missing files
        has_critical_missing = extraction.log_missing_files_errors(missing_files)
        
        # Fail the entire process if files are missing for NME or FINOB
        if has_critical_missing:
            nme_finob_missing = [
                f for f in missing_files 
                if f['source_system'].upper() in ['NME', 'FINOB']
            ]
            error_summary = (
                f"Critical files missing after deadline: "
                f"{', '.join([f['file_name'] for f in nme_finob_missing])}"
            )
            extraction.append_to_process_log(
                source_system="",
                comments=error_summary,
                status="Failed"
            )
            raise NonSSFExtractionError(
                NonSSFStepStatus.INIT_CHECKS,
                additional_info=error_summary
            )


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
    # Initialize extraction and process log
    extraction = ExtractNonSSFData(spark, run_month=run_month)
    extraction.initialize_process_log(run_id=run_id)
    
    # Start the process
    extraction.append_to_process_log(
        comments="",
        source_system="",
        status="Started"
    )

    try:
        # Check for missing files after deadline
        _check_and_handle_missing_files(extraction)
        
        # Get all files from basel-nonssf-landing container and place static data
        files_per_delivery_entity = extraction.get_all_files()
        if not files_per_delivery_entity:
            logger.error("No files found in basel-nonssf-landing container. ")
        else:
            logger.info(f"Processing {len(files_per_delivery_entity)} source files")

        logger.info(files_per_delivery_entity)

        # Process each file
        for file in files_per_delivery_entity:
            _process_single_file(extraction, file)

        # Complete the process after all trigger files
        extraction.append_to_process_log(
            comments="", 
            source_system="", 
            status="Completed"
        )
        
    except NonSSFExtractionError as e:
        # Log overall process failure
        extraction.append_to_process_log(
            source_system="",
            comments=str(e.additional_info) if hasattr(e, 'additional_info') else str(e),
            status="Failed"
        )
        raise
