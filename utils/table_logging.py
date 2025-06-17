from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from src.month_setup.metadata_log_tables import get_log_table_schemas
from src.utils.get_env import get_catalog
from src.utils.logging_util import get_logger

logger = get_logger()


def write_to_log(
    spark: SparkSession,
    run_month: str,
    record: dict,
    log_table: str = "process_log",
) -> None:
    """Writes record to a given log table

    Args:
        spark
        record: dict containing the fields and values for the log entry
            fields must have the same names as in the log table
        log_table: default = process_log
    """
    schema = get_log_table_schemas(log_table)[log_table]
    catalog = get_catalog(spark)
    log_entry = [record]
    log_entry_df = spark.createDataFrame(
        log_entry,
        schema=schema,
    )
    try:
        log_entry_df.write.mode("append").saveAsTable(
            f"{catalog}.log_{run_month}.{log_table}"
        )
    except AnalysisException:
        logger.exception(
            f"Error writing to process log table log_{run_month}.{log_table}"
        )


def get_result(result: bool | None) -> str:
    """Get string representation of process result.

    Args:
        result (bool | None): boolean or None result of
            process step

    Returns:
        str: String representation of process result.
            Can be `"SUCCESS"`, `"FAILED"` or `"NA"`

    """
    if result is None:
        return "NA"
    return "SUCCESS" if result else "FAILED"
