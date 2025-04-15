from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from src.month_setup.metadata_log_tables import get_log_table_schemas
from src.utils.logging_util import get_logger

logger = get_logger()


def write_to_log(
    spark: SparkSession,
    record: dict,
) -> None:
    """Writes record to process_log

    Args:
        spark
        record: dict containing the following keys
            Mandatory:
                process: component name
                status: Started/Completed/Failed
                run_month: yyyymm
                stage: staging/integration/etc
            Optional:
                source_system
                run_id
                workflow: parent workflow that triggered this component
                comments: str with details/link in case of failure
    """
    schema = get_log_table_schemas("process_log")["process_log"]
    # Dict to assign values
    run_month = record.get("run_month")
    if not run_month or not record.get("process") or not record.get("status"):
        logger.error("run_month, process and status are mandatory in record")
    log_entry = [
        {
            "RunID": record.get("run_id", 1),
            "Timestamp": datetime.now(tz=timezone.utc),
            "Workflow": record.get("workflow", ""),
            "Component": record.get("process"),
            "SourceSystem": record.get("source_system", ""),
            "Layer": record.get("stage"),
            "Status": record.get("status"),
            "Comments": record.get("comments", ""),
        }
    ]

    log_entry_df = spark.createDataFrame(
        log_entry,
        schema=schema,
    )
    try:
        log_entry_df.write.mode("append").saveAsTable(f"log_{run_month}.process_log")
    except AnalysisException as e:
        logger.exception(
            "Error writing to process log table log_{run_month}.process_log"
        )
        logger.exception(str(e).split(";")[0])
