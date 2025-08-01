from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

from abnamro_bsrc_etl.staging.extract_base import ExtractStagingData
from abnamro_bsrc_etl.staging.status import NonSSFStepStatus
from abnamro_bsrc_etl.utils.export_parquet import export_to_parquet
from abnamro_bsrc_etl.utils.get_env import get_container_path
from abnamro_bsrc_etl.utils.logging_util import get_logger
from abnamro_bsrc_etl.utils.table_logging import get_result

logger = get_logger()


@dataclass
class ExtractNonSSFData(ExtractStagingData):
    """Non-SSF source data from basel-sourcing-nonssf storage account."""

    staging_flow_name: str = "nonssf"
    source_container: str = "basel-sourcing-nonssf"
    file_delivery_status: type[NonSSFStepStatus] = NonSSFStepStatus

    def __post_init__(self) -> None:
        super().__post_init__()

    def check_file_expected_status(self, file_name: str) -> bool:
        """Check if file is in expected or redelivery status.
        
        Args:
            file_name (str): Name of the source file (without extension).
            
        Returns:
            bool: True if file is in expected or redelivery status
        """
        file_status_info = (
            self.meta_data.filter(col("SourceFileName") == file_name)
            .select("FileDeliveryStep")
            .collect()
        )
        
        if not file_status_info:
            return False
            
        file_delivery_step = file_status_info[0]["FileDeliveryStep"]
        
        # Check against enum values instead of string descriptions
        expected_steps = [
            NonSSFStepStatus.EXPECTED.value,
            NonSSFStepStatus.REDELIVERY.value
        ]
        
        return file_delivery_step in expected_steps

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
            list[str]: Updated list including copied static files.
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
            
            # Check if file is already delivered this month
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
                try:
                    processed_files = [
                        file.path
                        for file in self.dbutils.fs.ls(processed_folder)
                        if file.name.startswith(file_name)
                    ]
                except Exception:
                    logger.exception(
                        f"Error accessing processed folder for {file_name}"
                    )
                    continue
                    
                if not processed_files:
                    logger.error(
                        f"File {file_name} not delivered and not found in "
                        f"{source_system.upper()}/processed folder. "
                        f"Deadline was {deadline_date}."
                    )
                    continue
                    
                # Get the latest processed file
                latest_processed_file = max(processed_files)
                try:
                    # Copy the file to the static folder
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
                            f"Static file copied from processed folder "
                            f"after deadline ({deadline_date})."
                        ),
                    )
                except Exception:
                    logger.exception(
                        f"Failed to copy {file_name} from processed folder"
                    )
                    
        return new_files

    def check_missing_files_after_deadline(self) -> list[dict[str, str]]:
        """Check for files that are missing after their deadline.
        
        For NME and FINOB, checks if expected files with reached deadlines are present.
        For LRD_STATIC, the check is already done in place_static_data method.

        Returns:
            list[dict[str, str]]: List of dictionaries with missing file information
                containing 'source_system', 'file_name', and 'deadline'.
        """
        missing_files = []
        
        # Get all expected files from metadata for NME and FINOB only
        expected_files = (
            self.meta_data
            .filter(col("SourceSystem").isin(["nme", "NME", "finob", "FINOB"]))
            .select("SourceSystem", "SourceFileName", "Deadline")
            .distinct()
            .collect()
        )
        
        # Get current list of files in the source folders
        current_files = {}
        for source_system in ["NME", "FINOB"]:
            source_folder = f"{self.source_container_url}/{source_system}"
            try:
                current_files[source_system.upper()] = [
                    Path(f.path).stem 
                    for f in self.dbutils.fs.ls(source_folder)
                    if not f.isDir() and not f.name.endswith("/")
                ]
            except Exception:
                logger.exception(f"Error accessing {source_system} folder")
                current_files[source_system.upper()] = []
        
        # Check each expected file
        for file_info in expected_files:
            source_system = file_info["SourceSystem"].upper()
            file_name = file_info["SourceFileName"]
            
            # Skip if no deadline is set
            if not file_info["Deadline"]:
                continue
                
            deadline_reached, deadline_date = self.check_deadline_reached(file_name)
            
            # Check if file is missing and deadline has been reached
            if deadline_reached and file_name not in current_files.get(source_system, []):
                missing_files.append({
                    "source_system": source_system,
                    "file_name": file_name,
                    "deadline": deadline_date
                })
                    
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
                f"File {missing_file['file_name']} from "
                f"{missing_file['source_system']} is missing after "
                f"deadline ({missing_file['deadline']})"
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
        the processed folder if their deadline has been reached.

        Returns:
            list[dict[str, str]]: List of dictionaries containing file names and their
            corresponding source systems.
        """
        all_files = {}
        
        # Get expected files from metadata once
        expected_files = [
            row["SourceFileName"]
            for row in self.meta_data.select("SourceFileName").collect()
        ]
        
        for subfolder in ["NME", "FINOB", "LRD_STATIC"]:
            # Get all files from the subfolder
            try:
                raw_files = [
                    p.path
                    for p in self.dbutils.fs.ls(f"{self.source_container_url}/{subfolder}")
                    if not p.isDir() and not p.name.endswith("/")
                ]
            except Exception:
                logger.exception(f"Error accessing {subfolder} folder")
                raw_files = []
            
            # Filter files to only include those in metadata
            valid_files = []
            for file in raw_files:
                # Check if the file has a matching record in the metadata
                if Path(file).stem not in expected_files:
                    logger.warning(
                        f"File {Path(file).stem} not found in metadata. "
                        "Please check if it should be delivered."
                    )
                    continue
                
                # File is valid, add to list and update log
                valid_files.append(file)
                self.update_log_metadata(
                    source_system=subfolder,
                    key=Path(file).stem,
                    file_delivery_status=NonSSFStepStatus.RECEIVED,
                )
            
            all_files[subfolder] = valid_files
        
        # Handle LRD_STATIC files - copy from processed folder if deadline reached
        all_files["LRD_STATIC"] = self.place_static_data(all_files["LRD_STATIC"])
        
        # Convert to list of dictionaries
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
