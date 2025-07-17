from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    lower,
    when,
)
from pyspark.sql.utils import AnalysisException

from abnamro_bsrc_etl.dq.dq_validation import DQValidation
from abnamro_bsrc_etl.staging.status import (
    DialStepStatus,
    NonSSFStepStatus,
    SSFStepStatus,
    StepStatusClassTypes,
    StepStatusInstanceTypes,
)
from abnamro_bsrc_etl.utils.get_dbutils import get_dbutils
from abnamro_bsrc_etl.utils.get_env import get_catalog, get_container_path
from abnamro_bsrc_etl.utils.logging_util import get_logger
from abnamro_bsrc_etl.utils.parameter_utils import standardize_delivery_entity
from abnamro_bsrc_etl.utils.table_logging import get_result, write_to_log

logger = get_logger()


@dataclass
class ExtractStagingData:
    """Managing staging data extraction processes in Databricks.

    This class handles metadata and log table table names, reads data from the catalog,
    and manages container URLs for staging flows. It is designed to work with
    Databricks utilities and Spark SQL.

    Attributes:
        spark (SparkSession): The Spark session used for data processing.
        run_month (str): The month of the data run, formatted as YYYYMM.
        staging_flow_name (str): The name of the staging flow (e.g., "ssf_flow").
        file_delivery_status
            (type[DialStepStatus] | type[NonSSFStepStatus] | type[SSFStepStatus]):
            The current delivery status.
        source_container (str | None): The name of the source container, if applicable.
    """

    spark: SparkSession
    run_month: str
    staging_flow_name: str
    file_delivery_status: StepStatusClassTypes
    source_container: str | None = None

    def __post_init__(self) -> None:
        """Post-initialization method for setting up instance variables.

        This method initializes attributes required for the staging flow, including
        database utilities, catalog information, metadata and log paths, and container
        URLs. It also sets up the lookup flag for SSF flows.

        Attributes:
            dbutils: Utility object for interacting with Databricks File System (DBFS).
            catalog (str): The catalog name retrieved from the Spark session.
            meta_data_table_name (str): Path to the metadata table in the catalog.
            log_data_table_name (str): Path to the log table in the catalog.
            meta_data (DataFrame): Metadata table loaded as a Spark DataFrame.
            log_data (DataFrame): Log table loaded as a Spark DataFrame.
            source_container_url (str): URL of the source container.
                (specific to DIAL and Non-SSF flows).
            lookup_found (bool): Indicates whether the lookup was found
                (specific to SSF flow).
        """
        self.dbutils = get_dbutils()
        self.catalog: str = get_catalog(self.spark)
        self.meta_data_table_name, self.log_data_table_name = (
            self.get_log_metadata_table_name(
                self.catalog, self.run_month, self.staging_flow_name, prefix=prefix
            )
            for prefix in ["metadata", "log"]
        )
        self.meta_data: DataFrame = self.spark.read.table(self.meta_data_table_name)
        self.log_data: DataFrame = self.spark.read.table(self.log_data_table_name)
        self.source_container_url: str = (
            get_container_path(self.spark, container=self.source_container)
            if self.source_container
            else ""
        )

    @staticmethod
    def get_log_metadata_table_name(
        catalog: str,
        run_month: str,
        staging_flow_name: str,
        prefix: str,
    ) -> str:
        """Get the log or metadata table name, incl catalog and schema."""
        return f"{catalog}.{prefix}_{run_month}.{prefix}_{staging_flow_name}"

    @staticmethod
    def write_table_with_exception(data: DataFrame, full_table_name: str) -> str:
        """Write table to catalog with error handling.

        Args:
            data (DataFrame): Input DataFrame
            full_table_name (str): Fully qualified table name
        """
        try:
            msg = f"Loaded to {full_table_name}"
            data.write.mode("overwrite").saveAsTable(full_table_name)
        except AnalysisException:  # pragma: no cover
            msg = f"ERROR - Failed loading to {full_table_name}"
            logger.exception(msg)
        return msg

    def update_metadata(
        self,
        file_delivery_status: StepStatusInstanceTypes,
        key: str,
        ssf_table: str | None = None,
    ) -> None:
        """Update metadata table with file delivery status and step information.

        This method updates the `FileDeliveryStatus` and `FileDeliveryStep` columns in
        the metadata table based on the `file_delivery_status` type and conditions
        derived from the `delivery_entity` and `table_name`. The updated metadata table
        is then written back to the specified path.

        Args:
            file_delivery_status (SSFStepStatus | DialStepStatus | NonSSFStepStatus):
                The status of the file delivery, which determines the type of update to
                perform.
                - SSFStepStatus: Used for SSF updates, requiring both `delivery_entity`
                and `table_name`.
                - DialStepStatus: Used for Dial updates, requiring only
                    `delivery_entity`.
                - NonSSFStepStatus: Used for NonSSF updates, requiring `delivery_entity`
                    (processed as a file stem).

            key (str): The delivery entity or source file name used to
                identify the row(s) to update.
                - For SSFStepStatus, this is the `DeliveryEntity`.
                - For DialStepStatus and NonSSFStepStatus, this is the `SourceFileName`.

            ssf_table (str | None, optional): The SSF table name used for SSF
                updates. Defaults to None.

        Raises:
            TypeError: If the `file_delivery_status` type is invalid or unsupported.

        Side Effects:
            - Updates the `FileDeliveryStatus` and `FileDeliveryStep` columns in the
            metadata table.
            - Writes the updated metadata table back to the specified path.
        """
        if isinstance(file_delivery_status, SSFStepStatus):
            condition = (col("DeliveryEntity") == lit(key)) & (
                lower("SSFTableName") == lit(ssf_table)
            )
        elif isinstance(file_delivery_status, DialStepStatus | NonSSFStepStatus):
            condition = col("SourceFileName") == key
        else:
            unvalid_type = type(file_delivery_status).__name__
            msg = f"Invalid file_delivery_status type: {unvalid_type}"
            raise TypeError(msg)

        self.meta_data = self.meta_data.withColumns(
            {
                "FileDeliveryStatus": when(
                    condition, file_delivery_status.get_description()
                ).otherwise(col("FileDeliveryStatus")),
                "FileDeliveryStep": when(
                    condition, file_delivery_status.value
                ).otherwise(col("FileDeliveryStep")),
            }
        )
        self.write_table_with_exception(self.meta_data, self.meta_data_table_name)

    def update_log_metadata(
        self,
        key: str,
        file_delivery_status: StepStatusInstanceTypes,
        **kwargs,  # noqa: ANN003,
    ) -> None:
        """Update log status in log and metadata tables.

        This method writes a log entry to the appropriate log table and updates the
        metadata table based on the provided delivery entity and file delivery status.
        Additional keyword arguments can be passed to customize the log entry.

        Args:
            delivery_entity (str): The delivery entity or source file name used to
                identify the row(s) to update.
            file_delivery_status (DialStepStatus | NonSSFStepStatus | SSFStepStatus):
                The status of the file delivery, which determines the type of update
                to perform.
            lookup_found (bool, optional): Indicates whether the lookup was found in
                the metadata. Defaults to True.
            **kwargs: Additional keyword arguments to customize the log entry.

        Side Effects:
            - Writes a log entry to the appropriate log table.
            - Updates the metadata table based on the delivery entity and file
                delivery status (only if lookup is found for SSF flow).
        """
        logger.info(
            f"FileDeliveryStatus: {file_delivery_status.get_description()} "
            f"for {key}"
        )
        write_to_log(
            self.spark,
            self.run_month,
            record=self.get_log_entry(
                file_delivery_status=file_delivery_status,
                delivery_entity=key,
                **kwargs,
            ),
            log_table=f"log_{self.staging_flow_name}",
        )
        self.update_metadata(
            key=key,
            file_delivery_status=file_delivery_status,
            ssf_table=kwargs.get("ssf_table"),
        )

    def get_log_entry(
        self,
        delivery_entity: str,
        file_delivery_status: StepStatusInstanceTypes,
        result: str = "",
        comment: str = "",
        ssf_table: str = "",
        source_system: str = "",
        xsd_version: str = "",
        delivery_set: str = "",
        ssf_entity_name: str = "",
        snapshot_date: str = "",
        delivery_number: int | None = None,
    ) -> dict[str, str | int]:
        """Generate a log entry dictionary for logging updates.

        This method constructs a log entry based on the provided delivery entity,
        file delivery status, and other optional parameters. The structure of the
        log entry varies depending on the type of `file_delivery_status`
        (DialStepStatus, NonSSFStepStatus, or SSFStepStatus).

        Args:
            delivery_entity (str): The delivery entity or source file name used to
                identify the row(s) to update.
            file_delivery_status (DialStepStatus | NonSSFStepStatus | SSFStepStatus):
                The status of the file delivery, which determines the type of log
                entry to construct.
            result (str, optional): The result of the process step. Defaults to an
                empty string.
            comment (str, optional): Additional comments for process logging.
                Defaults to an empty string.
            ssf_table (str | None, optional): The SSF table name for SSF updates.
                Defaults to an empty string.
            source_system (str, optional): The source system name for DIAL
                and NonSSF updates. Defaults to an empty string.
            xsd_version (str, optional): The XSD version for SSF updates.
                Defaults to an empty string.
            delivery_set (str, optional): The delivery set for SSF updates.
                Defaults to None.
            ssf_entity_name (str, optional): The SSF entity name for SSF
                updates. Defaults to an empty string.
            snapshot_date (str, optional): The snapshot date for DIAL updates.
                Required for DialStepStatus. Defaults to an empty string.
            delivery_number (int | None, optional): The delivery number for SSF
                updates. Defaults to None.

        Returns:
            dict[str, str | int]: A dictionary representing the log entry, with keys
                and values specific to the type of `file_delivery_status`.
        """
        generic_log_entry = {
            "FileDeliveryStep": file_delivery_status.value,
            "FileDeliveryStatus": file_delivery_status.get_description(),
            "Result": result,
            "LastUpdatedDateTimestamp": datetime.now(tz=timezone.utc),
            "Comment": comment,
        }

        def get_max_delivery_number(data: DataFrame) -> int:
            return data.selectExpr("max(DeliveryNumber)").collect()[0][0] or 0

        if isinstance(file_delivery_status, DialStepStatus | NonSSFStepStatus):
            delivery_number_query = self.log_data.filter(
                (col("SourceFileName") == lit(delivery_entity))
                & (
                    col("FileDeliveryStatus")
                    == lit(file_delivery_status.get_description())
                )
                & (col("FileDeliveryStep") == lit(file_delivery_status.value))
            )
            max_delivery_number = get_max_delivery_number(delivery_number_query)
            log_entry = {
                **generic_log_entry,
                "SourceSystem": source_system,
                "SourceFileName": delivery_entity,
                "DeliveryNumber": max_delivery_number,
            }
            if isinstance(file_delivery_status, DialStepStatus):
                # For DIAL there is an additional filter and entry for SnapshotDate
                log_entry["DeliveryNumber"] = get_max_delivery_number(
                    delivery_number_query.filter(
                        col("SnapShotDate") == lit(snapshot_date)
                    )
                )
                log_entry = {**log_entry, "SnapshotDate": snapshot_date}

        elif isinstance(file_delivery_status, SSFStepStatus):
            log_entry = {
                **generic_log_entry,
                "SSFTableName": ssf_table,
                "DeliveryEntity": delivery_entity,
                "RedeliveryNumber": delivery_number,
                "SSFEntityName": ssf_entity_name,
                "DeliverySet": delivery_set,
                "XSDVersion": xsd_version,
            }
        return log_entry

    def save_to_stg_table(
        self,
        data: DataFrame,
        stg_table_name: str,
        **kwargs: str,
    ) -> bool:
        """Save the input PySpark DataFrame to the staging schema in Unity Catalog.

        This method writes the provided DataFrame to the specified staging table in UC.
        It updates the log metadata with the result of the operation and returns
        whether the operation was successful.

        Args:
            data (DataFrame): The PySpark DataFrame to be saved.
            stg_table_name (str): The name of the staging table in Unity Catalog.
            **kwargs (str): Additional keyword arguments for log metadata updates.

        Returns:
            bool: True if the DataFrame was successfully saved, False otherwise.
        """
        full_path = f"{self.catalog}.stg_{self.run_month}.{stg_table_name}"
        comment = self.write_table_with_exception(data, full_path)
        result = not comment.startswith("ERROR")
        self.update_log_metadata(
            file_delivery_status=self.file_delivery_status.LOADED_STG,
            result=get_result(result),
            comment=comment,
            **self.update_kwargs(**kwargs),
        )
        return result

    def validate_data_quality(
        self,
        stg_table_name: str,
        schema_name: str = "stg",
        dq_check_folder: str = "dq_checks",
        **kwargs: str,
    ) -> bool:
        """Perform Data Quality (DQ) validation for the loaded staging table.

        This method calls `DQValidation().checks()` from
        `abnamro_bsrc_etl.dq.dq_validation` to run
        any available DQ checks if a checks file is provided. It updates the log
        metadata with the results of the validation.

        Args:
            file_delivery_status (StepStatusClassTypes): The current delivery status of
                the file.
            stg_table_name (str): The name of the loaded table in the staging schema.
                Only the table name should be provided, without the schema prefix.
            schema_name (str, optional): The schema name prefix.
                Defaults to "stg".
            dq_check_folder (str, optional): The folder containing DQ check files.
                Defaults to "dq_checks".
            **kwargs (str): Additional keyword arguments, such as:
                - source_system (str): The source system of the data.
                - delivery_entity (str): The delivery entity, formatted as
                    in "Instagram Views".
                - file_name (str): SourceFileName as in DIAL or Non SSF.

        Returns:
            bool: True if DQ checks were successful or if no checks were defined,
                False otherwise.
        """
        result = DQValidation(
            self.spark,
            table_name=stg_table_name,
            schema_name=schema_name,
            run_month=self.run_month,
            source_system=standardize_delivery_entity(
                kwargs.get("source_system", kwargs.get("delivery_entity", ""))
            ),
            dq_check_folder=dq_check_folder,
        ).checks()

        self.update_log_metadata(
            file_delivery_status=self.file_delivery_status.CHECKED_DQ,
            result=get_result(result),
            comment=f"{get_result(result)}, see log_dq_validation",
            **self.update_kwargs(**kwargs),
        )
        return result in (True, None)

    @staticmethod
    def update_kwargs(**kwargs: str) -> dict:
        """Update keyword arguments by removing keys and setting the delivery entity.

        Removes "file_name" and "delivery_entity" keys from the input kwargs and sets
        the "key" key to the value of "delivery_entity" or "file_name" if
        not already present.

        Args:
            **kwargs (str): Arbitrary keyword arguments.

        Returns:
            dict: Updated dictionary with modified "delivery_entity" and filtered keys.
        """
        return {
            **{
                k: v
                for k, v in kwargs.items()
                if k not in ["file_name", "delivery_entity"]
            },
            "key": kwargs.get("delivery_entity", kwargs.get("file_name", "")),
        }
