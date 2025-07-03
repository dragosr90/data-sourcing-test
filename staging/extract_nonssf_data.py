from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.config.exceptions import NonSSFExtractionError
from src.staging.extract_base import ExtractStagingData
from src.staging.status import NonSSFStepStatus
from src.utils.export_parquet import export_to_parquet
from src.utils.get_env import get_container_path
from src.utils.logging_util import get_logger
from src.utils.table_logging import get_result

logger = get_logger()


@dataclass
class ExtractNonSSFData(ExtractStagingData):
    """Non-SSF source data from basel-sourcing-nonssf storage account."""

    staging_flow_name: str = "nonssf"
    source_container: str = "basel-sourcing-nonssf"
    file_delivery_status: type[NonSSFStepStatus] = NonSSFStepStatus

    def __post_init__(self) -> None:
        super().__post_init__()

    def initial_checks(
        self,
        file_name: str,
        source_system: str,
    ) -> bool:
        """Perform initial checks for the source file.

        Validates whether the extension of the source file matches the expected format
        specified in the metadata. Ensures the file is in the correct format before
        proceeding with further processing.

        Args:
            file_name (str): Name of the source file to validate.
            source_system (str): Name of the source system.

        Returns:
            bool: True if the file extension matches the expected format.
        """
        # Check if extension matches expected format
        expected_file_format = (
            self.meta_data.filter(col("SourceFileName") == Path(file_name).stem)
            .select("SourceFileFormat")
            .collect()[0]["SourceFileFormat"]
        )
        if not file_name.endswith(expected_file_format):
            msg = (
                f"File {file_name} does not match "
                f"expected format {expected_file_format}."
            )
            logger.error(msg)
            result = False
        else:
            msg = ""
            result = True
        self.update_log_metadata(
            source_system=source_system,
            key=Path(file_name).stem,
            file_delivery_status=NonSSFStepStatus.INIT_CHECKS,
            result=get_result(result),
            comment=msg,
        )
        return result

    def place_static_data(
        self, new_files: list[str], *, deadline_passed: bool = False
    ) -> list[str]:
        """Handle static data files for the LRD_STATIC source system.

        Loops over metadata entries for LRD_STATIC files and checks if they are
        delivered this month. If not, copies the files from the processed folder to the
        static folder only after deadline has passed and only for files that are still
        expected.

        Args:
            new_files (list[str]): List of files found in the source container. This
                list will be updated with files copied from the processed folder.
            deadline_passed (bool): Whether the deadline has passed. Only copy files
                after deadline has passed.

        Returns:
            list[str]: List of copied static files.
        """
        source_system = "lrd_static"  # Changed to lowercase to match metadata
        expected_files = (
            self.meta_data.where(f"SourceSystem = '{source_system}'")
            .select("SourceFileName", "SourceFileFormat")
            .distinct()
            .collect()
        )
        static_folder = f"{self.source_container_url}/{source_system.upper()}"
        processed_folder = (
            f"{self.source_container_url}/{source_system.upper()}/processed"
        )

        for file in expected_files:
            file_name = file["SourceFileName"]
            extension = file["SourceFileFormat"]

            # Check if file is already delivered this month
            if file_name not in [Path(f).stem for f in new_files]:
                # Only proceed with copying if deadline has passed
                if not deadline_passed:
                    logger.info(
                        f"File {file_name} not delivered but deadline not reached yet. "
                        "Skipping copy from processed folder."
                    )
                    continue

                # Check if the file is expected (still in metadata as expected)
                expected_status = (
                    self.meta_data.filter(col("SourceFileName") == file_name)
                    .select("FileDeliveryStep")
                    .collect()
                )

                # Use NonSSFStepStatus enum values for comparison
                if not expected_status or expected_status[0][
                    "FileDeliveryStep"
                ] not in [
                    NonSSFStepStatus.EXPECTED.value,
                    NonSSFStepStatus.REDELIVERY.value,
                ]:
                    logger.info(
                        f"File {file_name} is not in expected status. "
                        "Skipping copy from processed folder."
                    )
                    continue

                # Check if the file is already in the processed folder
                # Let any OSError propagate - don't catch and continue
                processed_files = [
                    file.path
                    for file in self.dbutils.fs.ls(processed_folder)
                    if file.name.startswith(file_name)
                ]

                if not processed_files:
                    logger.error(
                        f"File {file_name} not delivered and not found in "
                        f"{source_system.upper()}/processed folder."
                    )
                    continue

                latest_processed_file = max(processed_files)
                if self.dbutils.fs.ls(latest_processed_file):
                    # Copy the file to the static folder
                    processed_file = f"{processed_folder}/{file_name}"
                    target_file = f"{static_folder}/{file_name}{extension}"

                    try:
                        self.dbutils.fs.cp(latest_processed_file, target_file)
                        logger.info(
                            f"Copied {file_name} to static folder after deadline passed. "  # noqa: E501
                            f"Source: {latest_processed_file}, Target: {target_file}"
                        )
                        new_files.append(target_file)

                        self.update_log_metadata(
                            source_system=source_system.upper(),
                            key=Path(file_name).stem,
                            file_delivery_status=NonSSFStepStatus.RECEIVED,
                            comment=(
                                f"Static file {processed_file} copied from processed "
                                f"folder after deadline."
                            ),
                        )
                    except OSError as e:
                        error_msg = f"Failed to copy {latest_processed_file} to {target_file}: {e!s}"  # noqa: E501
                        logger.exception(error_msg)
                        raise NonSSFExtractionError(
                            NonSSFStepStatus.RECEIVED, additional_info=error_msg
                        ) from e

        return new_files

    def get_all_files(
        self, *, deadline_passed: bool = False, deadline_date: datetime | None = None
    ) -> list[dict[str, str]]:
        """Retrieve all files from the source container along with their source systems.

        Filters files based on metadata records. If a file is not found in the metadata,
        it is removed from the list. For LRD_STATIC files, missing files are copied from
        the processed folder only after deadline has passed and only for expected files.

        Args:
            deadline_passed (bool): Whether the deadline has passed
            deadline_date (datetime | None): The deadline date

        Returns:
            list[dict[str, str]]: List of dictionaries containing file names and their
            corresponding source systems.
        """
        all_files = {}
        for subfolder in ["NME", "FINOB", "LRD_STATIC"]:
            try:
                all_files[subfolder] = [
                    p.path
                    for p in self.dbutils.fs.ls(
                        f"{self.source_container_url}/{subfolder}"
                    )
                    if (not p.isDir() or p.name.endswith(".parquet"))
                ]
            except OSError as e:
                logger.warning(f"Could not access folder {subfolder}: {e}")
                all_files[subfolder] = []

            expected_files = [
                row["SourceFileName"]
                for row in self.meta_data.filter(
                    col("SourceSystem")
                    == subfolder.lower()  # Use lowercase for comparison
                )
                .select("SourceFileName")
                .collect()
            ]

            # Create a copy of the list to iterate over while modifying the original
            files_to_check = all_files[subfolder].copy()
            for file in files_to_check:
                # Check if the file has a matching record in the metadata
                if Path(file).stem not in expected_files:
                    logger.warning(
                        f"File {Path(file).stem} not found in metadata. "
                        "Please check if it should be delivered."
                    )
                    all_files[subfolder].remove(file)
                    continue
                self.update_log_metadata(
                    source_system=subfolder,
                    key=Path(file).stem,
                    file_delivery_status=NonSSFStepStatus.RECEIVED,
                )

        # Handle LRD_STATIC files with deadline logic
        all_files["LRD_STATIC"] = self.place_static_data(
            all_files["LRD_STATIC"], deadline_passed=deadline_passed
        )

        # Log deadline information
        if deadline_date:
            deadline_str = deadline_date.strftime("%Y-%m-%d %H:%M:%S UTC")
            if deadline_passed:
                logger.info(
                    f"Deadline has passed ({deadline_str}). "
                    f"LRD_STATIC files copied from processed folder."
                )
            else:
                logger.info(
                    f"Deadline not yet reached ({deadline_str}). "
                    f"LRD_STATIC files will not be copied."
                )

        return [
            {"source_system": source_system, "file_name": file_name}
            for source_system in all_files.keys()
            for file_name in all_files[source_system]
        ]

    def check_deadline_violations(
        self,
        files_per_delivery_entity: list[dict[str, str]],
    ) -> None:
        """Check for deadline violations for FINOB and NME files.

        Raises error if deadline has passed and expected files are missing.

        Args:
            files_per_delivery_entity: List of files found

        Raises:
            NonSSFExtractionError: If deadline violations are found
        """
        current_dt = datetime.now(tz=timezone.utc)

        # Get expected files for FINOB and NME from metadata with their deadlines
        finob_nme_expected = (
            self.meta_data.filter(
                self.meta_data.SourceSystem.isin(["finob", "nme"])  # Use lowercase
            )
            .select("SourceFileName", "SourceSystem", "Deadline")
            .collect()
        )

        # Get delivered files for FINOB and NME
        delivered_files = {
            Path(file["file_name"]).stem: file["source_system"].lower()
            for file in files_per_delivery_entity
            if file["source_system"].lower() in ["finob", "nme"]
        }

        # Check for missing files
        missing_files = []
        for row in finob_nme_expected:
            expected_file = row["SourceFileName"]
            source_system = row["SourceSystem"]
            deadline = row["Deadline"]

            if expected_file not in delivered_files and deadline:
                # Convert deadline to datetime
                if isinstance(deadline, str):
                    deadline_dt = datetime.strptime(deadline, "%Y-%m-%d").replace(
                        tzinfo=timezone.utc
                    )
                else:
                    deadline_dt = datetime.combine(
                        deadline, datetime.min.time()
                    ).replace(tzinfo=timezone.utc)

                # Only report violation if deadline has passed
                if current_dt >= deadline_dt:
                    deadline_str = deadline_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                    missing_files.append(
                        {
                            "file": f"{source_system}/{expected_file}",
                            "deadline": deadline_str,
                            "source_system": source_system,
                        }
                    )
                    logger.error(
                        f"Deadline passed ({deadline_str}): Missing expected file "
                        f"{expected_file} from {source_system}"
                    )

        if missing_files:
            # First, update metadata for ALL missing files
            for missing_file_info in missing_files:
                source_system = missing_file_info["source_system"]
                file_path = missing_file_info["file"]
                deadline = missing_file_info["deadline"]

                # Update log metadata with deadline information
                self.update_log_metadata(
                    source_system=source_system.upper(),
                    key=Path(file_path.split("/")[1]).stem,
                    file_delivery_status=NonSSFStepStatus.RECEIVED,
                    result="ERROR",
                    comment=f"Deadline passed ({deadline}): Missing expected file {file_path.upper()}",  # noqa: E501
                )

            # Create error message with all missing files
            missing_files_list = [
                f"{mf['file']} (deadline: {mf['deadline']})" for mf in missing_files
            ]
            error_msg = (
                f"Deadline violation: Missing files after deadline - "
                f"{', '.join(missing_files_list)}"
            )

            # Now raise the exception with the comprehensive error message
            raise NonSSFExtractionError(
                NonSSFStepStatus.RECEIVED, additional_info=error_msg
            )

    def get_deadline_from_metadata(
        self, source_system: str, file_name: str
    ) -> datetime | None:
        """Get deadline date from metadata for a specific file.

        Args:
            source_system: Source system name
            file_name: File name to check

        Returns:
            Deadline date from metadata or None if not found
        """
        deadline_row = (
            self.meta_data.filter(
                (col("SourceSystem") == source_system.lower())
                & (col("SourceFileName") == Path(file_name).stem)
            )
            .select("Deadline")
            .collect()
        )

        if deadline_row and deadline_row[0]["Deadline"]:
            # Assuming the Deadline column is a date type, convert to datetime
            deadline_date = deadline_row[0]["Deadline"]
            if isinstance(deadline_date, str):
                return datetime.strptime(deadline_date, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
            # If it's already a date/datetime object
            return datetime.combine(deadline_date, datetime.min.time()).replace(
                tzinfo=timezone.utc
            )

        return None

    def convert_to_parquet(self, source_system: str, file_name: str) -> bool:
        """Convert a source file to Parquet format and save it in the target container.

        Reads the source file based on its format and delimiter, converts it to Parquet,
        and saves it in the run month container.
        Supports .csv, .txt, and .parquet formats.

        Args:
            source_system (str): Name of the source system (e.g. LRD_STATIC, NME, FINOB)
            file_name (str): Path to the source file.

        Returns:
            bool: True if the conversion was successful, False otherwise.
        """
        file_info = (
            self.meta_data.filter(col("SourceFileName") == Path(file_name).stem)
            .select("SourceFileFormat", "SourceFileDelimiter")
            .collect()[0]
        )
        source_file_format = file_info["SourceFileFormat"]
        source_file_delimiter = file_info["SourceFileDelimiter"]
        # Read the file with the given format and delimiter
        if source_file_format in (".csv", ".txt"):
            data = self.spark.read.csv(
                file_name, sep=source_file_delimiter, header=True
            )
        elif source_file_format == ".parquet":
            data = self.spark.read.parquet(file_name)
        else:
            logger.error(
                f"Unsupported file format: {source_file_format}. "
                "Only .csv, .txt and .parquet are supported."
            )
            return False
        result = export_to_parquet(
            self.spark,
            data,
            run_month=self.run_month,
            file_name=Path(file_name).stem,
            folder=f"sourcing_landing_data/NON_SSF/{source_system}",
        )

        self.update_log_metadata(
            source_system=source_system,
            key=Path(file_name).stem,
            file_delivery_status=NonSSFStepStatus.CONVERTED_PARQUET,
            result=get_result(result),
        )
        return result

    def extract_from_parquet(self, source_system: str, file_name: str) -> DataFrame:
        """Extract data from a Parquet file.

        Reads the Parquet file from the target container and returns it as a PySpark
        DataFrame.

        Args:
            source_system (str): Name of the source system.
            file_name (str): Name of the Parquet file to extract.

        Returns:
            DataFrame: PySpark DataFrame containing the extracted data.
        """
        return self.spark.read.parquet(
            f"{get_container_path(self.spark, self.run_month)}"
            f"/sourcing_landing_data/NON_SSF/{source_system}/{Path(file_name).stem}.parquet"
        )

    def get_staging_table_name(self, file_name: str) -> str:
        """Retrieve the staging table name for a given source file.

        Queries the metadata to find the staging table name associated with source file.

        Args:
            file_name (str): Name of the source file.

        Returns:
            str: Name of the staging table.
        """
        return (
            self.meta_data.filter(col("SourceFileName") == Path(file_name).stem)
            .select("StgTableName")
            .collect()[0][0]
        )

    def move_source_file(
        self,
        source_system: str,
        file_name: str,
    ) -> bool:
        """Move a source file to the processed folder and append timestamp to its name.

        This method moves the source file to the processed folder, ensuring the file
        name includes a timestamp for tracking purposes.

        Args:
            source_system (str): Name of the source system.
            file_name (str): Path to the source file.

        Returns:
            bool: True if the move was successful, False otherwise.
        """
        source_data_path = file_name
        export_folder = f"{self.source_container_url}/{source_system}/processed"
        target_data_path = (
            f"{export_folder}/{Path(file_name).stem}__"
            f"{datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S')}"
            f"{Path(file_name).suffix}"
        )
        try:
            result = self.dbutils.fs.mv(source_data_path, target_data_path)
            comment = f"Moved source file {source_data_path} to {target_data_path}"
        except Exception:  # noqa: BLE001 # pragma: no cover
            result = False
            comment = (
                f"Failed moving source file {source_data_path} to {target_data_path}"
            )
        self.update_log_metadata(
            source_system=source_system,
            key=Path(file_name).stem,
            file_delivery_status=NonSSFStepStatus.MOVED_SRC,
            result=get_result(result),
            comment=comment,
        )
        return result
