from dataclasses import dataclass

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame
from pyspark.sql.functions import array, array_join
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException

from src.staging.extract_base import ExtractStagingData
from src.staging.status import DialStepStatus
from src.utils.export_parquet import export_to_parquet
from src.utils.get_env import get_container_path
from src.utils.logging_util import get_logger
from src.utils.table_logging import get_result

logger = get_logger()


@dataclass
class ExtractDialData(ExtractStagingData):
    """ETL DIAL source data from source storage account based on trigger files."""

    staging_flow_name: str = "dial_schedule"
    source_container: str = "dial-sourcing"
    file_delivery_status: type[DialStepStatus] = DialStepStatus

    TRIGGER_FOLDER: str = "dial_trigger"
    ARCHIVE_FOLDER: str = "dial_trigger_archive"

    def __post_init__(self) -> None:
        super().__post_init__()
        self.trigger_data: DataFrame = self._read_trigger_data()

    def _read_trigger_data(self) -> DataFrame:
        """Read trigger data from the source container."""
        return self.spark.read.json(
            f"{self.source_container_url}/{self.TRIGGER_FOLDER}",
            schema=StructType(
                [
                    StructField("dial_source_file_name", StringType()),
                    StructField("dial_source_system", StringType()),
                    StructField("dial_source_file_snapshot_datetime", StringType()),
                    StructField("dial_notification_datetime", StringType()),
                    StructField("dial_consumer_name", StringType()),
                    StructField("dial_target_file_full_path", StringType()),
                    StructField("dial_target_file_row_count", StringType()),
                ]
            ),
        )

    def get_matching_meta_trigger_data(
        self,
        meta_data_source_filename_col: str = "SourceFileName",
        meta_data_snapshotdate: str = "SnapshotDate",
        trigger_data_source_filename_col: str = "dial_source_file_name",
        trigger_snapshotdate: str = "dial_source_file_snapshot_datetime",
    ) -> DataFrame:
        """Match the snapshot date from metadata table with trigger data.

        Left Join on trigger data. So if there is no match the values of
        SourceFileName and SnapshotDate are NULL, so we can archive.
        these files. The original trigger file name is derived by using
        consumer_name, system, file_name, snapshot_datetime and
        notification_datetime columns.

        Returns:
            DataFrame: Matching trigger files on SourceFileName and SnapshotDate.
        """
        meta = self.meta_data.filter(
            self.meta_data["FileDeliveryStep"].isin(
                [DialStepStatus.EXPECTED.value, DialStepStatus.REDELIVERY.value]
            )
        )
        return self.trigger_data.join(
            meta,
            on=[
                meta[meta_data_source_filename_col]
                == self.trigger_data[trigger_data_source_filename_col],
                meta[meta_data_snapshotdate] == self.trigger_data[trigger_snapshotdate],
            ],
            how="left",
        ).select(
            "*",
            array_join(
                array(
                    "dial_consumer_name",
                    "dial_source_system",
                    "dial_source_file_name",
                    "dial_source_file_snapshot_datetime",
                    "dial_notification_datetime",
                ),
                "_",
            ).alias("trigger_file_name"),
        )

    @staticmethod
    def get_file_mapping(
        data: DataFrame,
        file_name_column: str = "dial_source_file_name",
        column_naming: None | dict = None,
    ) -> dict[str, dict[str, str]]:
        """Get the Data File location from the json trigger file.

        Args:
            data (DataFrame): Matching data
            file_name_column (str, optional): Column name with source file name.
                Defaults to "dial_source_file_name".
            column_naming (None | dict, optional): Mapping of columns to mapping keys.
                Defaults to None.

        Returns:
            dict: Mapping of file names with `data_path` to source data,
                expected `row_count` of data from  `data_path` and,
                `stg_table_name` of output staging table.
        """
        column_naming = (
            column_naming
            if column_naming
            else {
                "data_path": "dial_target_file_full_path",
                "row_count": "dial_target_file_row_count",
                "stg_table_name": "StgTableName",
            }
        )
        return {
            row[file_name_column]: {k: row[v] for k, v in column_naming.items()}
            for row in data[[file_name_column, *column_naming.values()]].collect()
        }

    @staticmethod
    def get_triggers(
        data: DataFrame, *, matching: bool = True
    ) -> dict[str, dict[str, str]]:
        """Get triggers or archive file mappings.

        Args:
            data (DataFrame): Input full trigger data from trigger folder.
            matching (bool, optional): True if we want the matching trigger files,
                False if we want to get the archive files. Defaults to True.

        Returns:
            dict[str, dict[str, str]]: Mapping of trigger `trigger_file_name` with
                mapping of `source_system`, `file_name`, and  `snapshot_date`.
        """
        return {
            row["trigger_file_name"]: {
                "source_system": row["dial_source_system"],
                "file_name": row["dial_source_file_name"],
                "snapshot_date": row["dial_source_file_snapshot_datetime"],
            }
            for row in data.filter(
                "SourceFileName is not null" if matching else "SourceFileName is null"
            )
            .distinct()
            .collect()
        }

    def extract_from_blob_storage(
        self,
        data_path: str,
        source_system: str,
        file_name: str,
        snapshot_date: str,
    ) -> DataFrame | None:
        """Extract from Azure blob storage.

        Args:
            data_path (str): Path to parquet input data, from JSON trigger file.
            source_system (str): Source System
            file_name (str): Data file name
            snapshot_date (str): Matching snapshot date in format YYYY-mm-dd

        Returns:
            DataFrame: PySpark DataFrame with input data from `data_path`.
        """
        try:
            data = self.spark.read.parquet(data_path)
            result = True
            comment = ""
            data.take(1)
        except (Py4JJavaError, AnalysisException):  # pragma: no cover
            result = False
            comment = f"Path does not exist: {data_path}"
            data = None

        self.update_log_metadata(
            source_system=source_system,
            key=file_name,
            snapshot_date=snapshot_date,
            file_delivery_status=DialStepStatus.EXTRACTED,
            result=get_result(result),
            comment=comment,
        )
        return data

    def validate_expected_row_count(
        self,
        data: DataFrame,
        expected_row_count: int | str,
        source_system: str,
        file_name: str,
        snapshot_date: str,
    ) -> bool:
        """Validate expected count of input data.

        Args:
            data (DataFrame): Input PySpark DataFrame
            expected_row_count (int | str): Expected count of input data
            source_system (str): Source System
            file_name (str): Data file name
            snapshot_date (str): Matching snapshot date in format YYYY-mm-dd

        Returns:
            bool: True if `expected_count == data.count()`.
        """
        received_count = data.count()
        expected_row_count = int(expected_row_count)
        result = received_count == int(expected_row_count)
        if not result:
            comment = f"Expected {expected_row_count}, received {received_count}"
            logger.error(comment)
        else:
            comment = f"Expected and received {expected_row_count} records"
        self.update_log_metadata(
            source_system=source_system,
            key=file_name,
            snapshot_date=snapshot_date,
            file_delivery_status=DialStepStatus.VALIDATED_ROW_COUNT,
            result=get_result(result),
            comment=comment,
        )
        return result

    def copy_to_blob_storage(
        self,
        data: DataFrame,
        source_system: str,
        file_name: str,
        snapshot_date: str,
    ) -> bool:
        """Copy parquet file to Azure Blob storage.

        Args:
            data (DataFrame): Input PySpark DataFrame
            expected_count (int | str): Expected count of input data
            source_system (str): Source System
            file_name (str): Data file name
            snapshot_date (str): Matching snapshot date in format YYYY-mm-dd

        Returns:
            bool: True if data is copied.
        """
        folder: str = f"sourcing_landing_data/DIAL/{source_system}"
        result = export_to_parquet(
            self.spark,
            data=data,
            run_month=self.run_month,
            folder=folder,
            file_name=file_name,
        )
        self.update_log_metadata(
            source_system=source_system,
            key=file_name,
            snapshot_date=snapshot_date,
            file_delivery_status=DialStepStatus.COPIED_PARQUET,
            result=get_result(result),
        )
        return result

    def move_trigger_file(
        self,
        trigger_file_name: str,
        source_system: str,
        file_name: str,
        snapshot_date: str,
    ) -> bool:
        """Move JSON trigger file from source to target container in Azure blob storage.

        Args:
            trigger_file_name (str): Input JSON trigger file name
            source_system (str): Source System
            file_name (str): Data file name
            snapshot_date (str): Matching snapshot date in format YYYY-mm-dd
        """
        trigger_data_path = get_path(
            [
                self.source_container_url,
                self.TRIGGER_FOLDER,
                f"{trigger_file_name}.json",
            ]
        )
        folder: str = f"sourcing_landing_data/DIAL/{source_system}"
        month_container_url = get_container_path(
            spark=self.spark, container=self.run_month
        )
        export_folder = f"{month_container_url}/{folder}"
        output_trigger_path = f"{export_folder}_trigger/{trigger_file_name}.json"
        try:
            result = self.dbutils.fs.mv(trigger_data_path, output_trigger_path)
            comment = f"Moved trigger file {trigger_data_path} to {output_trigger_path}"
        except Exception:  # noqa: BLE001 # pragma: no cover
            result = False
            comment = f"Failed moving trigger file {trigger_data_path} to {output_trigger_path}"  # noqa: E501
        self.update_log_metadata(
            source_system=source_system,
            key=file_name,
            snapshot_date=snapshot_date,
            file_delivery_status=DialStepStatus.MOVED_JSON,
            result=get_result(result),
            comment=comment,
        )
        return result

    def move_archive_file(
        self,
        trigger_file_name: str,
        trigger_file_folder: str = TRIGGER_FOLDER,
        trigger_file_archive_folder: str = ARCHIVE_FOLDER,
    ) -> bool:
        """Move archive `trigger_file_name` in `dial_trigger_archive` folder.

        Args:
            trigger_file_name (str): Input JSON trigger file name
            source_system (str): Source System
            file_name (str): Data file name
            snapshot_date (str): Matching snapshot date in format YYYY-mm-dd
            trigger_file_folder (str): Input trigger file folder
            trigger_file_archive_folder (str): Output / Archive trigger file folder
        """
        file_path, archive_path = (
            get_path(
                [
                    self.source_container_url,
                    folder,
                    f"{trigger_file_name}.json",
                ]
            )
            for folder in [trigger_file_folder, trigger_file_archive_folder]
        )
        try:
            result = self.dbutils.fs.mv(file_path, archive_path)
            logger.info(f"Moved trigger file {file_path} to {archive_path}")
        except Exception:  # noqa: BLE001   # pragma: no cover
            result = False
            logger.info(f"Failed moving trigger file {file_path} to {archive_path}")

        return result


def get_path(hierarchy: list[str], sep: str = "/") -> str:
    """Get path from list of nested directories.

    Args:
        hierarchy (list[str]): List of nested directories (from left to right).
        sep (str, optional): Character to treat as the delimiter. Defaults to "/".

    Returns:
        str: Output path
    """
    return sep.join(hierarchy)
