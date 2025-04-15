from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.utils import AnalysisException

from src.config.constants import PROJECT_ROOT_DIRECTORY
from src.month_setup.dial_derive_snapshotdate import derive_snapshot_date
from src.utils.logging_util import get_logger

logger = get_logger()


# Table schemas
def get_metadata_table_schemas(table: str = "") -> dict:  # pragma: no cover
    metadata_tables = {
        "metadata_dial_schedule": StructType(
            [
                StructField("SourceSystem", StringType(), nullable=False),
                StructField("SourceFileName", StringType(), nullable=False),
                StructField("DeliverySchedule", StringType(), nullable=False),
                StructField(
                    "DeliveryScheduleDescription", StringType(), nullable=False
                ),
                StructField("StgTableName", StringType(), nullable=False),
                StructField("SnapshotDate", StringType(), nullable=False),
                StructField("FileDeliveryStatus", StringType(), nullable=False),
            ]
        ),
        "metadata_ssf_xsd_change": StructType(
            [
                StructField("NewXSDVersion", StringType(), nullable=False),
                StructField("OldXSDVersion", StringType(), nullable=False),
                StructField("ChangeType", StringType(), nullable=False),
                StructField("NewEntityName", StringType(), nullable=False),
                StructField("OldEntityName", StringType(), nullable=True),
                StructField("NewAttributeName", StringType(), nullable=True),
                StructField("OldAttributeName", StringType(), nullable=True),
                StructField("NewEntityKey", StringType(), nullable=True),
                StructField("OldEntityKey", StringType(), nullable=True),
            ]
        ),
        "metadata_ssf_entities": StructType(
            [
                StructField("SSFEntityName", StringType(), nullable=False),
                StructField("SSFTableName", StringType(), nullable=False),
                StructField("DeliveryEntity", StringType(), nullable=False),
                StructField("DeliverySet", StringType(), nullable=False),
                StructField("FileDeliveryStatus", StringType(), nullable=False),
            ]
        ),
        "metadata_ssf_abc_list": StructType(
            [
                StructField("SSF_Entity_Name", StringType(), nullable=False),
                StructField("Attribute_Name", StringType(), nullable=False),
                StructField("Key", StringType(), nullable=False),
                StructField("XSD_Version", StringType(), nullable=False),
                StructField("TEAM", StringType(), nullable=True),
                StructField("Table_Name", StringType(), nullable=True),
            ]
        ),
    }
    if table:
        return {table: metadata_tables[table]}
    return metadata_tables


def get_log_table_schemas(table: str = "") -> dict:  # pragma: no cover
    log_tables = {
        "log_dial_schedule": StructType(
            [
                StructField("SourceSystem", StringType(), nullable=False),
                StructField("SourceFileName", StringType(), nullable=False),
                StructField("SnapshotDate", StringType(), nullable=False),
                StructField("FileDeliveryStatus", StringType(), nullable=False),
                StructField("DeliveryNumber", IntegerType(), nullable=False),
                StructField(
                    "LastUpdatedDateTimestamp", TimestampType(), nullable=False
                ),
                StructField("Comment", StringType(), nullable=False),
            ]
        ),
        "log_ssf_entities": StructType(
            [
                StructField("SSFEntityName", StringType(), nullable=False),
                StructField("SSFTableName", StringType(), nullable=False),
                StructField("DeliveryEntity", StringType(), nullable=False),
                StructField("DeliverySet", StringType(), nullable=False),
                StructField("XSDVersion", StringType(), nullable=True),
                StructField("RedeliveryNumber", IntegerType(), nullable=True),
                StructField("FileDeliveryStatus", StringType(), nullable=True),
                StructField("LastUpdatedDateTimestamp", TimestampType(), nullable=True),
                StructField("Comment", StringType(), nullable=True),
            ]
        ),
        "log_dq_validation": StructType(
            [
                StructField("Timestamp", TimestampType(), nullable=False),
                StructField("Schema", StringType(), nullable=False),
                StructField("SourceSystem", StringType(), nullable=True),
                StructField("TableName", StringType(), nullable=False),
                StructField("ProcessLogID", StringType(), nullable=True),
                StructField("CheckType", StringType(), nullable=False),
                StructField("CheckDetails", StringType(), nullable=False),
                StructField("CheckResult", StringType(), nullable=False),
                StructField("Error", StringType(), nullable=True),
            ]
        ),
        "process_log": StructType(
            [
                StructField("RunID", IntegerType(), nullable=False),
                StructField("Timestamp", TimestampType(), nullable=False),
                StructField("Workflow", StringType(), nullable=True),
                StructField("Component", StringType(), nullable=False),
                StructField("SourceSystem", StringType(), nullable=True),
                StructField("Layer", StringType(), nullable=False),  # Stage?
                StructField("Status", StringType(), nullable=False),
                StructField("Comments", StringType(), nullable=True),
            ]
        ),
    }
    if table:
        return {table: log_tables[table]}
    return log_tables


def create_metadata_log_tables(
    spark: SparkSession, month_no: str
) -> None:  # pragma: no cover
    """Creates the table structures for metadata and log tables
    and loads the month-independent metadata from the /metadata/*.csv files
    """
    for table_name, schema in get_metadata_table_schemas().items():
        try:
            metadata_table = spark.read.load(
                f"file:{PROJECT_ROOT_DIRECTORY}/metadata/{table_name}.csv",
                format="csv",
                sep=",",
                schema=schema,
            )
        except FileNotFoundError as e:
            logger.exception(
                f"File {table_name}.csv could not be found in "
                f"file:{PROJECT_ROOT_DIRECTORY}/metadata/"
            )
            logger.exception(str(e).split(";")[0])

        try:
            metadata_table.write.mode("overwrite").saveAsTable(
                f"metadata_{month_no}.{table_name}"
            )
        except AnalysisException as e:
            logger.exception(
                f"Error loading {table_name} to metadata_{month_no}.{table_name}"
            )
            logger.exception(str(e).split(";")[0])

        logger.info(f"Created table {table_name} in metadata_{month_no}")
    for table_name, schema in get_log_table_schemas().items():
        try:
            log_table = spark.createDataFrame([], schema=schema)
            log_table.write.mode("ignore").saveAsTable(f"log_{month_no}.{table_name}")
        except AnalysisException as e:
            logger.exception(
                f"Error loading {table_name} to log_{month_no}.{table_name}"
            )
            logger.exception(str(e).split(";")[0])

        logger.info(f"Created table {table_name} in log_{month_no}")


def derive_month_specific_fields(
    spark: SparkSession, month_no: str
) -> None:  # pragma: no cover
    """Derive any month specific fields and add those to the metadata tables.

    Derived attributes:
    - metadata_dial_schedule.SnapshotDate
    """
    # DIAL snapshotdate
    dial_schedule = spark.read.table(f"metadata_{month_no}.metadata_dial_schedule")

    derive_snapshot_date_udf = udf(
        lambda x: derive_snapshot_date(int(month_no[:-2]), int(month_no[-2:]), x)
    )
    dial_schedule_updated = dial_schedule.withColumn(
        "SnapshotDate",
        derive_snapshot_date_udf(dial_schedule["DeliverySchedule"]),
    ).withColumn("FileDeliveryStatus", lit("1 - Expected"))
    try:
        dial_schedule_updated.write.mode("overwrite").saveAsTable(
            f"metadata_{month_no}.metadata_dial_schedule"
        )
    except AnalysisException as e:
        logger.exception(f"Error updating metadata_{month_no}.metadata_dial_schedule")
        logger.exception(str(e).split(";")[0])
    logger.info(
        "Updated SnapshotDate and FileDeliveryStatus in metadata_dial_schedule"
        f" in metadata_{month_no}"
    )

    log_entry = [
        {
            "SourceSystem": row["SourceSystem"],
            "SourceFileName": row["SourceFileName"],
            "SnapshotDate": row["SnapshotDate"],
            "FileDeliveryStatus": row["FileDeliveryStatus"],
            "DeliveryNumber": 0,
            "LastUpdatedDateTimestamp": datetime.now(tz=timezone.utc),
            "Comment": "Snapshot date derived for schedule {schedule}".format(
                schedule=row["DeliverySchedule"]
            ),
        }
        for row in dial_schedule_updated.collect()
    ]

    log_entry_df = spark.createDataFrame(
        log_entry,
        schema=get_log_table_schemas("log_dial_schedule")["log_dial_schedule"],
    )
    try:
        log_entry_df.write.mode("append").saveAsTable(
            f"log_{month_no}.log_dial_schedule"
        )
    except AnalysisException as e:
        logger.exception(f"Error writing to log_{month_no}.log_dial_schedule")
        logger.exception(str(e).split(";")[0])


def tables_new_month(spark: SparkSession, month_no: str) -> None:  # pragma: no cover
    """Creates and loads the metadata and log tables for a given month number"""
    if not month_no:
        month_no = datetime.now(tz=timezone.utc).strftime("%Y%m")
    create_metadata_log_tables(spark, month_no)
    derive_month_specific_fields(spark, month_no)
    logger.info(f"Created and populated metadata and log tables for {month_no}")
