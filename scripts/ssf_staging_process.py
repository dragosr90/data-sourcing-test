# ruff: noqa: ERA001
import sys
from pathlib import Path

sys.path.append(f"{Path.cwd().parent.resolve()}")

import sys
from datetime import datetime, timezone
from typing import Literal

from pyspark.sql import SparkSession

from src.config.exceptions import SSFExtractionError
from src.config.process import ProcessLogConfig
from src.staging.extract_ssf_data import ExtractSSFData
from src.staging.status import SSFStepStatus
from src.utils.logging_util import get_logger
from src.utils.parameter_utils import standardize_delivery_entity
from src.utils.table_logging import write_to_log

logger = get_logger()


def ssf_load(
    spark: SparkSession,
    run_month: str,
    run_id: int = 1,
) -> None:
    """Full load of Instagram SSF data.

    Steps:
    1. Extract data from Instagram Views
    2. Checks record count to validate completeness
    3. Export data to Parquet file
    4. Convert XSD to latest format as needed
    5. Load data into staging tables
    6. Perform DQ checks if specified


    Args:
        spark: Spark session
        run_month: month number as "YYYYMM"
        run_id: Run ID, default = 1
    """

    base_record: dict[str, int | datetime | str] = {
        "RunID": run_id,
        "Timestamp": datetime.now(tz=timezone.utc),
        "Workflow": "Staging",
        "Component": "Instagram SSF",
        "Layer": "Staging",
    }

    log_config: ProcessLogConfig = {
        "spark": spark,
        "run_month": run_month,
        "record": base_record,
    }
    append_to_process_log(
        **log_config,
        source_system="",
        status="Started",
        comments="",
    )

    # Get updated expected delivery entities
    extraction = ExtractSSFData(spark, run_month=run_month)
    updated_delivery_entities = extraction.get_updated_delivery_entities()
    logger.info(f"New delivery entities: {updated_delivery_entities}")

    for delivery_entity in updated_delivery_entities:
        logger.info(f"Processing delivery entity: {delivery_entity}")
        append_to_process_log(
            **log_config,
            source_system=delivery_entity,
            status="Started",
            comments="",
        )

        # 1. Extract data based on Run ID and ABC list
        ssf_data = extraction.extract_from_view(
            delivery_entity=delivery_entity,
        )

        if not ssf_data:
            logger.info(f"No data extracted for {delivery_entity}. Continuing")
            continue

        logger.info(f"Extracted {len(ssf_data)} files from view")

        for ssf_table, data in ssf_data.items():
            lookup = extraction.get_lookup(delivery_entity, ssf_table)
            lookup_info = extraction.get_updated_log_info(lookup)
            # 2. Completeness checks
            if not extraction.validate_record_counts(delivery_entity, ssf_table, data):
                append_to_process_log(
                    **log_config,
                    source_system=delivery_entity,
                    status="Failed",
                    file_delivery_status=SSFStepStatus.VALIDATED_ROW_COUNT,
                    comments=f"for {ssf_table}",
                )
            logger.info(f"Completeness check passed for {ssf_table}")

            # 3. Export to Parquet
            if not extraction.export_to_storage(
                spark, delivery_entity, ssf_table, data
            ):
                append_to_process_log(
                    **log_config,
                    source_system=delivery_entity,
                    status="Failed",
                    file_delivery_status=SSFStepStatus.EXPORTED_PARQUET,
                    comments=f"for {ssf_table}",
                )
            logger.info(f"Exported {ssf_table} to Parquet")

        # 4. Pass dataframes to XSD conversion module, receive dict back
        ssf_data_new_xsd = extraction.convert_to_latest_xsd(delivery_entity, ssf_data)
        logger.info("Converted dataframes to latest XSD format")

        for ssf_table, data in ssf_data_new_xsd.items():
            # 5. Load into STG schema

            stg_table_name = (
                f"ssf_{ssf_table}_{standardize_delivery_entity(delivery_entity)}"
            )
            if not extraction.save_to_stg_table(
                data=data,
                stg_table_name=stg_table_name,
                ssf_table=ssf_table,
                delivery_entity=delivery_entity,
                **lookup_info,
            ):
                append_to_process_log(
                    **log_config,
                    source_system=delivery_entity,
                    status="Failed",
                    file_delivery_status=SSFStepStatus.LOADED_STG,
                    comments=f"for {ssf_table}",
                )
            logger.info(f"Loaded {ssf_table} into STG schema in {stg_table_name}")

            # 6. DQ checks
            if not extraction.validate_data_quality(
                delivery_entity=delivery_entity,
                stg_table_name=stg_table_name,
                ssf_table=ssf_table,
                **lookup_info,
            ):
                append_to_process_log(
                    **log_config,
                    source_system=delivery_entity,
                    status="Failed",
                    file_delivery_status=SSFStepStatus.CHECKED_DQ,
                    comments=f"for {ssf_table}",
                )
            logger.info(f"DQ checks passed for {stg_table_name}")

            # Log completed to ssf log
            extraction.update_log_metadata(
                ssf_table=ssf_table,
                key=delivery_entity,
                **lookup_info,
                file_delivery_status=SSFStepStatus.COMPLETED,
            )

        # Create the relevant empty tables for this delivery entity
        extraction.create_empty_tables(delivery_entity)

        append_to_process_log(
            **log_config,
            source_system=delivery_entity,
            status="Completed",
            comments="",
            file_delivery_status=SSFStepStatus.COMPLETED,
        )

    append_to_process_log(
        **log_config,
        source_system="",
        status="Completed",
        comments="",
        file_delivery_status=SSFStepStatus.COMPLETED,
    )


def append_to_process_log(
    spark: SparkSession,
    run_month: str,
    record: dict[str, int | datetime | str],
    source_system: str,
    comments: str,
    status: Literal["Completed", "Started", "Failed"] = "Completed",
    file_delivery_status: SSFStepStatus = SSFStepStatus.COMPLETED,
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
        SSFExtractionError: If status is "Failed".
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
        raise SSFExtractionError(file_delivery_status, additional_info=comments)


if __name__ == "__main__":
    # Get args:
    if len(sys.argv) not in [2, 3]:  # First argument is script name
        logger.error(
            "Incorrect number of parameters, expected 1 or 2: run_month[ run_id]"
        )
        sys.exit(-1)

    script, run_month, *run_id_list = sys.argv
    run_id = 1 if not run_id_list else int(run_id_list[0])
    ssf_load(spark, run_month, run_id)  # type: ignore[name-defined]
