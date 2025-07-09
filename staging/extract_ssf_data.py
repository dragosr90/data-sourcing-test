from calendar import monthrange
from dataclasses import dataclass

from pyspark.sql import DataFrame, Row, SparkSession, Window
from pyspark.sql.functions import (
    col,
    collect_list,
    concat_ws,
    lit,
    lower,
    max,
    regexp_replace,
    trim,
)
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

from src.staging.extract_base import ExtractStagingData
from src.staging.status import SSFStepStatus
from src.utils.export_parquet import export_to_parquet
from src.utils.get_env import get_env
from src.utils.logging_util import get_logger
from src.utils.parameter_utils import standardize_delivery_entity
from src.utils.table_logging import get_result

logger = get_logger()


@dataclass
class ExtractSSFData(ExtractStagingData):
    """SSF data extraction and preprocessing"""

    staging_flow_name: str = "ssf_entities"
    file_delivery_status: type[SSFStepStatus] = SSFStepStatus
    source_container = None

    def __post_init__(self) -> None:
        super().__post_init__()
        self.basel_catalog = self.catalog
        # Use Instagram production catalog if running on production, else acceptance
        insta_env = "p" if get_env(self.spark) == "p" else "a"
        self.instap_catalog = f"instap_{insta_env}"
        self.last_day_date = self._get_last_day_of_month(self.run_month)
        self.available_file_info = self._get_available_file_info()

    @staticmethod
    def _get_last_day_of_month(run_month: str) -> str:
        """Get the last day of the given month."""
        year = int(run_month[:4])
        month = int(run_month[4:])
        last_day = monthrange(year, month)[1]
        return f"{year}-{month:02d}-{last_day:02d}"

    def _get_run_process_table(self) -> DataFrame:
        """Get Run Process Table for the last day of the month."""
        return (
            self.spark.read.table(
                f"{self.instap_catalog}.ez_basel_vw.run_process_table"
            )
            .where(f"FileReportingDate = '{self.last_day_date}'")
            .withColumnRenamed("FileDeliveryEntity", "DeliveryEntity")
            .withColumn(
                "maxRunID",
                max("RunID").over(
                    Window.partitionBy(
                        "DeliveryEntity", "SSFTableName", "FileReportingDate"
                    )
                ),
            )
            .where("RunID = maxRunID")
            .alias("rpt")
        )

    def _get_available_file_info(self) -> DataFrame:
        """Retrieve available file information."""
        run_process_table = self._get_run_process_table()
        expected_ssf_entities = self.meta_data.where(
            col("FileDeliveryStep").isin(
                [SSFStepStatus.EXPECTED.value, SSFStepStatus.REDELIVERY.value]
            ),
        ).alias("mse")
        file_info = (
            run_process_table.join(
                expected_ssf_entities,
                [
                    lower("rpt.SSFTableName") == lower("mse.SSFTableName"),
                    lower("rpt.DeliveryEntity") == lower("mse.DeliveryEntity"),
                ],
                "inner",
            )
            .select(
                "rpt.RunID",
                "rpt.DeliveryEntity",
                "rpt.SSFTableName",
                regexp_replace(col("rpt.FileName"), "PAYLOAD.zip", "").alias(
                    "FileName"
                ),
                "rpt.XSDVersion",
                "rpt.DeliverySet",
                "rpt.RedeliveryNumber",
                "rpt.ActualRowCount",
                "mse.SSFEntityName",
                "mse.FileDeliveryStatus",
            )
            .withColumn(
                "FileNameParquet",
                concat_ws(
                    "_",
                    "FileName",
                    "rpt.XSDVersion",
                    "rpt.DeliverySet",
                    "rpt.RedeliveryNumber",
                ),
            )
            .orderBy("DeliveryEntity", "SSFTableName")
        )
        return self.spark.createDataFrame(file_info.collect(), schema=file_info.schema)

    def get_lookup(
        self,
        delivery_entity: str,
        ssf_table: str,
    ) -> dict[str, str]:
        """Retrieve lookup information for the given delivery entity and SSF table.

        This method fetches metadata from `available_file_info` based on the
        delivery entity and SSF table name. If no matching record is found,
        it generates default values for creating empty tables.

        Args:
            delivery_entity (str): The name of the delivery entity.
            ssf_table (str): The name of the SSF table.

        Returns:
            dict[str, str]: A dictionary containing lookup information, including
            entity name, edelivery number, delivery set, and XSD version.
        """
        try:
            lookup = (
                self.available_file_info.where(
                    (col("DeliveryEntity") == delivery_entity)
                    & (lower(col("SSFTableName")) == ssf_table)
                )
                .collect()[0]
                .asDict()
            )
        except IndexError:
            # If no matching record is found, it is for creating empty tables
            max_xsd_version = (
                self.spark.read.table(
                    f"{self.basel_catalog}.metadata_{self.run_month}.metadata_ssf_abc_list"
                )
                .where(lower(col("SSFTableName")) == ssf_table)
                .withColumn(
                    "MaxXSDVersion",
                    max("XSDVersion").over(Window.partitionBy("SSFTableName")),
                )
                .collect()[0]["MaxXSDVersion"]
            )
            lookup = {
                "SSFEntityName": "",
                "RedeliveryNumber": 0,
                "DeliverySet": "",
                "XSDVersion": max_xsd_version,
            }
        return lookup

    @staticmethod
    def get_updated_log_info(
        lookup: dict[str, str], row: Row | dict[str, str] | None = None
    ) -> dict[str, str]:
        """Get log information with lookup values.

        This method updates log information by combining values from the provided row
        and lookup dictionary. If a value is missing in the row, the corresponding value
        from the lookup dictionary is used.

        Args:
            lookup (dict[str, str]): A dictionary containing lookup values for log info.
            row (Row | dict[str, str] | None): A PySpark Row or dictionary containing
                log information to be updated. If None, an empty dictionary is used.

        Returns:
            dict[str, str]: A dictionary containing updated log information, including
            SSF entity name, delivery set, XSD version, and delivery number.
        """
        row = row.asDict() if isinstance(row, Row) else row
        row = {} if row is None else row
        return {
            "ssf_entity_name": row.get("SSFEntityName") or lookup["SSFEntityName"],
            "delivery_set": row.get("DeliverySet") or lookup["DeliverySet"],
            "xsd_version": row.get("XSDVersion") or lookup["XSDVersion"],
            "delivery_number": row.get("RedeliveryNumber")
            or lookup["RedeliveryNumber"],
        }

    def get_updated_delivery_entities(self) -> list[str]:
        """Get the delivery entities with new available data

        Checks for status 'Expected' or '9 Expected' in metadata
        and available data in the Instagram view

        Returns the list of delivery entites that are expected and available
        """
        return [
            row["DeliveryEntity"]
            for row in (
                self.available_file_info.select("DeliveryEntity").distinct().collect()
            )
        ]

    def extract_from_view(self, delivery_entity: str) -> dict[str, DataFrame]:
        """Extract available SSF data for a given delivery entity

        Reads the data from the Instagram table view, filtering rows on Delivery Entity
        and Run ID and selecting columns as given in the ABC list.
        Saves the data in a dataframe.

        Args:
            spark
            delivery_entity: format as given in Instagram Views

        Returns:
            Dictionary of dataframes per ssf_table_name, e.g. {'col': df}
        """

        metadata_ssf_abc_list = self.spark.read.table(
            f"{self.basel_catalog}.metadata_{self.run_month}.metadata_ssf_abc_list"
        ).where("AttributeName not like '%Custom%'")  # NOT SUPPORTED YET

        final_query_df = (
            self.available_file_info.alias("jd")
            .where(col("DeliveryEntity") == delivery_entity)
            .join(
                metadata_ssf_abc_list.alias("abc"),
                (lower(trim("jd.SSFTableName")) == lower(trim("abc.SSFTableName")))
                & (col("jd.XSDVersion") == col("abc.XSDVersion")),
                "inner",
            )
            .select(
                lower(trim("jd.SSFTableName")).alias("SSFTableName"),
                trim("jd.DeliveryEntity").alias("DeliveryEntity"),
                col("jd.FileName"),
                col("jd.XSDVersion").alias("XSDVersion"),
                col("jd.RunID"),
                col("jd.DeliverySet"),
                col("jd.RedeliveryNumber"),
                col("jd.SSFEntityName"),
                col("jd.FileDeliveryStatus"),
                col("abc.AttributeName"),
            )
        )

        aggregated_query_df = final_query_df.groupBy(
            "SSFTableName",
            "DeliveryEntity",
            "FileName",
            "RunID",
            "XSDVersion",
            "DeliverySet",
            "RedeliveryNumber",
            "SSFEntityName",
            "FileDeliveryStatus",
        ).agg(collect_list("AttributeName").alias("AttributeNames"))

        unique_combinations = aggregated_query_df.collect()

        if not unique_combinations:
            logger.info(
                "No matching records in the ABC list with the same filename and "
                f"XSD version for delivery entity {delivery_entity}"
            )
            return {}

        final_df = {}

        # Loop through each SSFTableName & DeliveryEntity
        for row in unique_combinations:
            ssf_table = row["SSFTableName"]
            file_delivery_entity = row["DeliveryEntity"]
            run_id = row["RunID"]
            lookup = self.get_lookup(file_delivery_entity, ssf_table)
            log_info = {
                "key": file_delivery_entity,
                "file_delivery_status": SSFStepStatus.EXTRACTED,
                "ssf_table": ssf_table,
                **self.get_updated_log_info(lookup, row),
            }
            column_list = [
                "FileDeliveryEntity",
                "FileReportingDate",
                *list(set(row["AttributeNames"])),
            ]

            # Check if the view exists
            if not self.spark.catalog.tableExists(
                f"{self.instap_catalog}.ez_basel_vw.{ssf_table}"
            ):
                self.update_log_metadata(
                    **log_info,
                    result="FAILED",
                    comment=f"View {self.instap_catalog}.ez_basel_vw.{ssf_table}"
                    " does not exist. Skipping query.",
                )
                continue

            try:
                query_result = (
                    self.spark.read.table(
                        f"{self.instap_catalog}.ez_basel_vw.{ssf_table}"
                    )
                    .select(column_list)
                    .where(
                        f"RunID = {run_id} "
                        f"and FileDeliveryEntity = '{file_delivery_entity}'"
                    )
                )
                final_df[ssf_table] = query_result
                result = True
            except AnalysisException:
                result = False
            self.update_log_metadata(
                **log_info,
                comment=f"Query execution failed for {ssf_table}" if not result else "",
                result=get_result(result),
            )

        return final_df

    def validate_record_counts(
        self, delivery_entity: str, ssf_table: str, data: DataFrame
    ) -> bool:
        """Validate the row count of the extracted data.

        This method retrieves the expected row count (`ActualRowCount`) from
        Instagram metadata and compares it to the actual row count of the provided
        DataFrame (`data.count()`).

        Args:
            delivery_entity (str): The name of the delivery entity
            ssf_table (str): The short format of the SSF table name, as in Instagram
            data (DataFrame): The PySpark DataFrame containing the extracted data.

        Returns:
            bool: True if the actual row count matches the expected row count.
        """
        expected_count_df = self.available_file_info.where(
            col("DeliveryEntity") == delivery_entity
        ).where(lower("SSFTableName") == ssf_table)

        if expected_count_df.count() != 1:
            logger.error(f"Not unique! {expected_count_df.count()} entries found")
        expected_count = expected_count_df.select("ActualRowCount").take(1)[0][
            "ActualRowCount"
        ]
        received_count = data.count()
        result = True
        comment = f"Expected and received {expected_count} records"
        if received_count != expected_count:
            comment = f"Expected {expected_count}, received {received_count} records"
            logger.error(f"Record count does not match. {comment}")
            result = False

        row = expected_count_df.collect()[0]
        lookup = self.get_lookup(delivery_entity, ssf_table)
        self.update_log_metadata(
            key=delivery_entity,
            file_delivery_status=SSFStepStatus.VALIDATED_ROW_COUNT,
            ssf_table=ssf_table,
            **self.get_updated_log_info(lookup, row),
            result=get_result(result),
            comment=comment,
        )
        return received_count == expected_count

    def export_to_storage(
        self, spark: SparkSession, delivery_entity: str, ssf_table: str, data: DataFrame
    ) -> bool:
        """Exports DataFrame to Parquet file with expected filename

        Retrieves expected filename and calls export_to_parquet functionality
        to export to sourcing_landing_data/INSTAGRAM/<delivery_entity>/<filename>
        on the `data` storage account.

        Returns boolean whether export was successful
        """
        row = (
            self.available_file_info.where(
                (col("DeliveryEntity") == delivery_entity)
                & (lower("SSFTableName") == ssf_table)
            )
        ).collect()[0]
        file_name_parquet = row["FileNameParquet"]
        result = export_to_parquet(
            spark,
            run_month=self.run_month,
            folder=f"sourcing_landing_data/INSTAGRAM/{delivery_entity}",
            file_name=file_name_parquet,
            data=data,
        )
        lookup = self.get_lookup(delivery_entity, ssf_table)
        self.update_log_metadata(
            key=delivery_entity,
            file_delivery_status=SSFStepStatus.EXPORTED_PARQUET,
            ssf_table=ssf_table,
            **self.get_updated_log_info(lookup, row),
            result=get_result(result),
            comment=f"Exported to {file_name_parquet}",
        )
        return result

    def convert_to_latest_xsd(
        self, delivery_entity: str, data: dict[str, DataFrame]
    ) -> dict[str, DataFrame]:
        """Converts all dataframes for certain delivery entity to the latest XSD version

        Checks if the XSDVersion is the latest. If not, looks up the transformations
        in metadata_ssf_xsd_change to convert the dataframe into the latest format.

        Args:
            spark
            delivery_entity: format as in Instagram Views
            data: dictionary of dataframes per ssf_table, e.g. {'col':df}

        Returns:
            dictionary of dataframes per ssf_table, e.g. {'col':df}
            with same tables as input `data`, but with the dataframes updated
            as defined

        Note:
            This method takes and returns all dataframes for a given delivery entity
            whereas the other methods work per dataframe.
            This is because there are XSD transformations (Move) that require more than
            one dataframe to be updated at the same time.
        """
        metadata_table = (
            f"{self.basel_catalog}.metadata_{self.run_month}.metadata_ssf_xsd_change"
        )
        changes_df = self.spark.read.table(metadata_table).join(
            self.available_file_info.where(col("DeliveryEntity") == delivery_entity)
            .selectExpr("XSDVersion as OldXSDVersion")
            .drop_duplicates(),
            on="OldXSDVersion",
            how="inner",
        )

        table_changes: dict[str, list[str]] = {}

        for row in changes_df.collect():
            change_type = row["ChangeType"]
            new_entity_name = row["NewEntityName"]
            old_entity_name = row["OldEntityName"]
            old_attr = row["OldAttributeName"]
            new_attr = row["NewAttributeName"]
            old_entity_key = row["OldEntityKey"]
            new_entity_key = row["NewEntityKey"]

            if new_entity_name not in data:
                logger.error(f"Skipping invalid or missing NewEntityName in row: {row}")
                continue

            table_changes[new_entity_name] = [
                *table_changes.get(new_entity_name, []),
                change_type,
            ]

            target_df = data[new_entity_name]

            if change_type == "Add":
                table_changes[new_entity_name] = [
                    *table_changes.get(new_entity_name, []),
                    f"Added {new_attr}",
                ]
                target_df = target_df.withColumn(new_attr, lit(None).cast(StringType()))
                data[new_entity_name] = target_df
                logger.info(f"Added '{new_attr}' to {new_entity_name}")

            elif change_type == "Remove":
                table_changes[new_entity_name] = [
                    *table_changes.get(new_entity_name, []),
                    f"Removed {old_attr}",
                ]
                target_df = target_df.drop(old_attr)
                # nothing happens if column does not exist
                data[new_entity_name] = target_df
                logger.info(f"Removed '{old_attr}' from '{new_entity_name}'")

            elif change_type == "Rename":
                table_changes[new_entity_name] = [
                    *table_changes.get(new_entity_name, []),
                    f"Renamed {old_attr} to {new_attr}",
                ]
                target_df = target_df.withColumnRenamed(old_attr, new_attr)
                # nothing happens if column does not exist
                data[new_entity_name] = target_df
                logger.info(
                    f"Renamed '{old_attr}' to '{new_attr}' in {new_entity_name}"
                )

            elif change_type == "Move":
                table_changes[new_entity_name] = [
                    *table_changes.get(new_entity_name, []),
                    f"Moved {old_entity_name}.{old_attr} "
                    f"to {new_entity_name}.{new_attr}",
                ]
                table_changes[old_entity_name] = [
                    *table_changes.get(new_entity_name, []),
                    f"Moved {old_entity_name}.{old_attr} "
                    f"to {new_entity_name}.{new_attr}",
                ]
                if old_entity_name not in data:
                    logger.error(f"Old entity {old_entity_name} not found")
                    continue
                source_df = data[old_entity_name]
                if old_attr.lower() not in [c.lower() for c in source_df.columns]:
                    logger.error(
                        f"Old attribute {old_attr} not found in {old_entity_name}"
                    )
                    continue
                moved_column = source_df.select(
                    col(old_entity_key), col(old_attr).alias(new_attr)
                )
                target_df = target_df.join(
                    moved_column,
                    lower(target_df[new_entity_key])
                    == lower(moved_column[old_entity_key]),
                    "left",
                ).drop(moved_column[old_entity_key])
                data[new_entity_name] = target_df
                source_df = source_df.drop(old_attr)
                data[old_entity_name] = source_df
                logger.info(
                    f"Moved '{old_attr}' in {old_entity_name} to "
                    f"'{new_attr}' in {new_entity_name}"
                )
        for table in data:
            # Find changes for certain table
            comment = "\n".join(table_changes.get(table, ["No changes done"]))
            lookup = self.get_lookup(delivery_entity=delivery_entity, ssf_table=table)
            self.update_log_metadata(
                key=delivery_entity,
                file_delivery_status=SSFStepStatus.XSD_CONVERTED,
                ssf_table=table,
                **self.get_updated_log_info(lookup),
                comment=comment,
            )
        return data

    def create_empty_tables(self, delivery_entity: str) -> None:
        """Creates empty staging tables for the delivery entity

        Creates empty staging tables for all SSF tables that are not expected
        for the delivery entity, but are expected for other delivery entities.
        This is used to ensure that all tables are available in the staging schema
        for every delivery entity.

        Args:
            delivery_entity: format as in Instagram Views
        """
        all_ssf_tables = self.meta_data.select("SSFTableName").distinct()
        # Get all SSF tables that are expected for other delivery entities
        ssf_tables = all_ssf_tables.subtract(
            self.meta_data.where(col("DeliveryEntity") == delivery_entity).select(
                "SSFTableName"
            )
        )

        metadata_ssf_abc_list = (
            self.spark.read.table(
                f"{self.basel_catalog}.metadata_{self.run_month}.metadata_ssf_abc_list"
            )
            .where("AttributeName not like '%Custom%'")  # NOT SUPPORTED YET
            .withColumn(
                "MaxXSDVersion",
                max("XSDVersion").over(Window.partitionBy("SSFTableName")),
            )
            .where(col("XSDVersion") == col("MaxXSDVersion"))
            .select("SSFTableName", "AttributeName")
        )

        column_lists = (
            metadata_ssf_abc_list.groupBy(
                "SSFTableName",
            )
            .agg(collect_list("AttributeName").alias("AttributeNames"))
            .join(ssf_tables, on="SSFTableName", how="inner")
            .collect()
        )

        for row in column_lists:
            column_list = [
                "FileDeliveryEntity",
                "FileReportingDate",
                *list(set(row["AttributeNames"])),
            ]
            ssf_table = row["SSFTableName"]
            stg_table_name = (
                f"ssf_{standardize_delivery_entity(delivery_entity)}_{ssf_table}"
            )
            try:
                query_result = (
                    self.spark.read.table(
                        f"{self.instap_catalog}.ez_basel_vw.{ssf_table}"
                    )
                    .select(column_list)
                    .where(lit(col=False))  # No data, just schema
                )
                query_result.write.mode("overwrite").saveAsTable(
                    f"{self.basel_catalog}.stg_{self.run_month}.{stg_table_name}"
                )
                comment = f"Created empty table {stg_table_name} in staging schema"
                logger.info(comment)
                result = True
            except AnalysisException:
                comment = f"ERROR - Failed creating empty table {stg_table_name}"
                logger.exception(comment)
                result = False

            lookup = self.get_lookup(delivery_entity, ssf_table)
            self.update_log_metadata(
                key=delivery_entity,
                file_delivery_status=SSFStepStatus.EXTRACTED,
                ssf_table=ssf_table,
                **self.get_updated_log_info(lookup, row),
                result=get_result(result),
                comment=comment,
            )
