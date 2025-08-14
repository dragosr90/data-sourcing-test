import sys
from pathlib import Path

sys.path.append(f"{Path.cwd().parent.resolve()}")

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp

from abnamro_bsrc_etl.config.exceptions import DelayedDialFilesError
from abnamro_bsrc_etl.staging.status import DialStepStatus
from abnamro_bsrc_etl.utils.get_env import get_catalog
from abnamro_bsrc_etl.utils.logging_util import get_logger

logger = get_logger()


def get_delayed(spark: SparkSession, run_month: str) -> list[Row]:
    """
    Check if there are any delayed dial files for the given month.
    A file is considered delayed if the snapshotdate is >1,5 day ago
    Noon on the next day

    Args:
        spark (SparkSession): The Spark session.
        run_month (str): The month to check for delayed dial files.

    Returns:
        bool: True if there are delayed dial files, False otherwise.
    """
    # Load the metadata table for dial, where snapshot date is >1,5 day ago
    delay_threshold = 36  # hours, 1.5 days
    metadata_df = (
        spark.read.table(
            f"{get_catalog(spark)}.metadata_{run_month}.metadata_dial_schedule"
        )
        .where(col("FileDeliveryStep") != DialStepStatus.COMPLETED.value)
        .select("SnapshotDate", "SourceFileName")
        .distinct()
    )

    return metadata_df.where(
        (
            current_timestamp().cast("long")
            - to_timestamp(col("SnapshotDate"), "yyyy-MM-dd").cast("long")
        )
        / 3600
        > delay_threshold
    ).collect()


def check_delayed(spark: SparkSession, run_month: str) -> tuple[bool, list[Row]]:
    delayed = get_delayed(spark=spark, run_month=run_month)
    if delayed:
        logger.error(f"Delayed dial files found for {run_month}:")
        for row in delayed:
            logger.error(
                f"SourceFileName: {row['SourceFileName']}, "
                f"Expected SnapshotDate: {row['SnapshotDate']}"
            )
        return True, delayed
    logger.info(f"No delayed dial files found for {run_month}.")
    return False, []


def run_check_delayed(spark: SparkSession, run_month: str) -> None:
    any_delayed, delayed = check_delayed(spark, run_month)
    if any_delayed:
        # Later we can send an email or raise an incident from here
        raise DelayedDialFilesError(run_month, delayed)
