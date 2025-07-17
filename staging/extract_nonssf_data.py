from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date

from abnamro_bsrc_etl.staging.extract_base import ExtractStagingData
from abnamro_bsrc_etl.staging.status import NonSSFStepStatus
from abnamro_bsrc_etl.utils.export_parquet import export_to_parquet
from abnamro_bsrc_etl.utils.get_env import get_container_path
from abnamro_bsrc_etl.utils.logging_util import get_logger
from abnamro_bsrc_etl.utils.table_logging import get_result, write_to_log

logger = get_logger()


@dataclass
class ExtractNonSSFData(ExtractStagingData):
    """Non-SSF source data from basel-sourcing-nonssf storage account."""

    staging_flow_name: str = "nonssf"
    source_container: str = "basel-sourcing-nonssf"
    file_delivery_status: type[NonSSFStepStatus] = NonSSFStepStatus

    def __post_init__(self) -> None:
        super().__post_init__()
        # Initialize process log record
        self.base_process_record: dict[str, int | datetime | str] = {}

    def initialize_process_log(self, run_id: int = 1) -> None:
        """Initialize the base process log record.

        Args:
            run_id (int, optional): Run ID. Defaults to 1.
        """
        self.base_process_record = {
            "RunID": run_id,
            "Timestamp": datetime.now(tz=timezone.utc),
            "Workflow": "Staging",
            "Component": "Non-SSF",
            "Layer": "Staging",
        }

    def append_to_process_log(
        self,
        source_system: str,
        comments: str,
        status: Literal["Completed", "Started", "Failed"] = "Completed",
        file_delivery_status: NonSSFStepStatus = NonSSFStepStatus.COMPLETED,
    ) -> None:
        """Append log entry to process log table.

        Args:
            source_system (str): Source System
            comments (str): Comment of step
            status (Literal["Completed", "Started", "Failed"]): Status of the step.
                Defaults to "Completed".
            file_delivery_status (NonSSFStepStatus): File delivery status.
                Defaults to NonSSFStepStatus.COMPLETED.
        """
        # Note: file_delivery_status is kept for API compatibility but not used in process log
        record = self.base_process_record.copy()
        record["Status"] = status
        record["Comments"] = comments
        record["SourceSystem"] = source_system
        
        write_to_log(
            spark=self.spark,
            run_month=self.run_month,
            record=dict(record),
            log_table="process_log",
        )

    def check_deadline_reached(self, file_name: str) -> tuple[bool, str]:
        """Check if the deadline has been reached for a given file.

        Args:
            file_name (str): Name of the source file (without extension).

        Returns:
            tuple[bool, str]: (deadline_reached, deadline_date_str)
                - deadline_reached: True if current date >= deadline
                - deadline_date_str: The deadline date as string
        """
        deadline_info = (
            self.meta_data.filter(col("SourceFileName") == file_name)
            .select("Deadline")
            .collect()
        )
        
        if not deadline_info or deadline_info[0]["Deadline"] is None:
            logger.warning(f"No deadline found for file {file_name}")
            return False, ""
        
        deadline_str = deadline_info[0]["Deadline"]
        # Parse deadline - assuming format YYYY-MM-DD
        deadline_date = datetime.strptime(deadline_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).date()
        current_date = datetime.now(timezone.utc).date()
        
        return current_date >= deadline_date, deadline_str

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

    def place_static_data(self, new_files: list[str]) -> list[str]:
        """Handle static data files for the LRD_STATIC source system.

        Loops over metadata entries for LRD_STATIC files and checks if they are
        delivered this month. If not, copies the files from the processed folder to the
        static folder ONLY if the deadline has been reached.

        Args:
            new_files (list[str]): List of files found in the source container. This
                list will be updated with files copied from the processed folder.

        Returns:
            list[str]: List of copied static files.
        """
        source_system = "lrd_static"
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
            
            if file_name not in [Path(f).stem for f in new_files]:
                # Check if deadline has been reached
                deadline_reached, deadline_date = self.check_deadline_reached(file_name)
                
                if not deadline_reached:
                    logger.info(
                        f"Deadline not reached for {file_name} (deadline: {deadline_date}). "
                        f"Skipping copy from processed folder."
                    )
                    continue
                
                # Deadline reached, try to copy from processed folder
                # Check if the file is already in the processed folder
                processed_files = [
                    file.path
                    for file in self.dbutils.fs.ls(processed_folder)
                    if file.name.startswith(file_name)
                ]
                if not processed_files:
                    logger.error(
                        f"File {file_name} not delivered and not found in "
                        f"{source_system.upper()}/processed folder. "
                        f"Deadline was {deadline_date}."
                    )
                    continue
                    
                latest_processed_file = max(processed_files)
                if self.dbutils.fs.ls(latest_processed_file):
                    # Copy the file to the static folder
                    processed_file = f"{processed_folder}/{file_name}"
                    self.dbutils.fs.cp(
                        latest_processed_file,
                        f"{static_folder}/{file_name}{extension}",
                    )
                    logger.info(
                        f"Copied {file_name} to static folder after deadline reached."
                    )
                    new_files.append(f"{static_folder}/{file_name}{extension}")
                    
                self.update_log_metadata(
                    source_system=source_system.upper(),
                    key=Path(file_name).stem,
                    file_delivery_status=NonSSFStepStatus.RECEIVED,
                    comment=(
                        f"Static file {processed_file} copied from process folder "
                        f"after deadline ({deadline_date})."
                    ),
                )
        return new_files

    def check_missing_files_after_deadline(self) -> list[dict[str, str]]:
        """Check for files that are missing after their deadline.

        Returns:
            list[dict[str, str]]: List of dictionaries with missing file information
                containing 'source_system', 'file_name', and 'deadline'.
        """
        missing_files = []
        
        # Get all expected files from metadata
        expected_files = (
            self.meta_data
            .select("SourceSystem", "SourceFileName", "Deadline")
            .distinct()
            .collect()
        )
        
        # Check each expected file
        for file_info in expected_files:
            source_system = file_info["SourceSystem"]
            file_name = file_info["SourceFileName"]
            
            # Skip if no deadline is set
            if not file_info["Deadline"]:
                continue
                
            deadline_reached, deadline_date = self.check_deadline_reached(file_name)
            
            if deadline_reached:
                # Check if file exists in the source folder
                source_folder = f"{self.source_container_url}/{source_system.upper()}"
                try:
                    existing_files = [
                        f.name for f in self.dbutils.fs.ls(source_folder)
                        if not f.isDir()
                    ]
                    file_found = any(
                        f.startswith(file_name) for f in existing_files
                    )
                    
                    if not file_found:
                        # For LRD_STATIC, also check if it was copied from processed
                        if source_system.upper() == "LRD_STATIC":
                            # Check if we already handled this in place_static_data
                            processed_folder = f"{source_folder}/processed"
                            processed_files = [
                                f.name for f in self.dbutils.fs.ls(processed_folder)
                                if f.name.startswith(file_name)
                            ]
                            if not processed_files:
                                missing_files.append({
                                    "source_system": source_system,
                                    "file_name": file_name,
                                    "deadline": deadline_date
                                })
                        else:
                            missing_files.append({
                                "source_system": source_system,
                                "file_name": file_name,
                                "deadline": deadline_date
                            })
                except Exception:
                    logger.exception(f"Error checking files in {source_folder}")
                    
        return missing_files

    def log_missing_files_errors(self, missing_files: list[dict[str, str]]) -> bool:
        """Log errors for missing files after deadline.

        Args:
            missing_files (list[dict[str, str]]): List of missing files info.

        Returns:
            bool: True if there are critical missing files (NME/FINOB), False otherwise.
        """
        has_critical_missing = False
        
        for missing_file in missing_files:
            error_msg = (
                f"File {missing_file['file_name']} from {missing_file['source_system']} "
                f"is missing after deadline ({missing_file['deadline']})"
            )
            logger.error(error_msg)
            
            # Log the missing file error
            self.update_log_metadata(
                source_system=missing_file['source_system'],
                key=missing_file['file_name'],
                file_delivery_status=NonSSFStepStatus.INIT_CHECKS,
                result="FAILED",
                comment=error_msg,
            )
            
            # Check if it's a critical source system
            if missing_file['source_system'].upper() in ['NME', 'FINOB']:
                has_critical_missing = True
                
        return has_critical_missing

    def get_all_files(self) -> list[dict[str, str]]:
        """Retrieve all files from the source container along with their source systems.

        Filters files based on metadata records. If a file is not found in the metadata,
        it is removed from the list. For LRD_STATIC files, missing files are copied from
        the processed folder.

        Returns:
            list[dict[str, str]]: List of dictionaries containing file names and their
            corresponding source systems.
        """
        all_files = {}
        for subfolder in ["NME", "FINOB", "LRD_STATIC"]:
            all_files[subfolder] = [
                p.path
                for p in self.dbutils.fs.ls(f"{self.source_container_url}/{subfolder}")
                if (not p.isDir() or p.name.endswith(".parquet"))
            ]
            expected_files = [
                row["SourceFileName"]
                for row in self.meta_data.select("SourceFileName").collect()
            ]
            for file in all_files[subfolder]:
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
        all_files["LRD_STATIC"] = self.place_static_data(all_files["LRD_STATIC"])
        return [
            {"source_system": source_system, "file_name": file_name}
            for source_system in all_files.keys()
            for file_name in all_files[source_system]
        ]

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
