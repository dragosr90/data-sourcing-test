from enum import Enum


class BaseStepStatus(Enum):
    """Base class for step statuses with shared functionality.

    Provides methods for retrieving descriptions of statuses, combining common and
    data-specific descriptions, and ensuring subclasses implement required methods.
    """

    @classmethod
    def get_all_descriptions(cls: type["BaseStepStatus"]) -> dict[int, str]:
        """Get all descriptions for the Enum.

        Returns:
            dict[int, str]: A dictionary mapping all Enum values to their descriptions.
        """
        return cls._descriptions()

    def get_description(self) -> str:
        """Get the description for the current Enum value.

        Returns:
            str: The description for the current Enum value.
        """
        return self._descriptions()[self.value]

    @classmethod
    def _descriptions(cls: type["BaseStepStatus"]) -> dict[int, str]:
        """Combine common and data-specific descriptions.

        Returns:
            dict[int, str]: A dictionary mapping all Enum values to their descriptions.
        """
        return dict(
            sorted(
                {
                    **cls._data_specific_descriptions(),
                    **cls._common_descriptions(),
                }.items()
            )
        )

    @classmethod
    def _data_specific_descriptions(cls: type["BaseStepStatus"]) -> dict[int, str]:
        """Define data-specific descriptions for the Enum.

        Subclasses must override this method to provide their specific descriptions.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        msg = "Subclasses must implement the `_data_specific_descriptions` method."
        raise NotImplementedError(msg)

    @classmethod
    def _common_descriptions(cls: type["BaseStepStatus"]) -> dict[int, str]:
        """Define common descriptions shared across all subclasses.

        Returns:
            dict[int, str]: Mapping of common Enum values to their descriptions.
        """
        return {
            0: "Expected",
            5: "Loaded Staging table",
            6: "Checked Data Quality",
            7: "Completed",
            8: "Redelivery expected",
        }


class DialStepStatus(BaseStepStatus):
    """Enum class representing the various steps in the DIAL data processing pipeline.

    Attributes:
        EXPECTED (int): The step is expected.
        EXTRACTED (int): Raw parquet data has been extracted.
        VALIDATED_ROW_COUNT (int): The expected row count has been validated.
        COPIED_PARQUET (int): Parquet data has been copied to the storage account.
        MOVED_JSON (int): The JSON trigger file has been moved.
        LOADED_STG (int): Data has been loaded into the staging area.
        CHECKED_DQ (int): Data quality checks have been performed.
        COMPLETED (int): The process has been completed.
        REDELIVERY (int): A redelivery is expected.

    Example:
        >>> status = DialStepStatus.EXTRACTED
        >>> print(status.value)
        1
        >>> print(status.get_description())
        "Extracted raw parquet data"
    """

    EXPECTED = 0
    EXTRACTED = 1
    VALIDATED_ROW_COUNT = 2
    COPIED_PARQUET = 3
    MOVED_JSON = 4
    LOADED_STG = 5
    CHECKED_DQ = 6
    COMPLETED = 7
    REDELIVERY = 8

    @classmethod
    def _data_specific_descriptions(cls: type["DialStepStatus"]) -> dict[int, str]:
        """Define data-specific descriptions for the DIAL pipeline."""
        return {
            cls.EXTRACTED.value: "Extracted raw parquet data",
            cls.VALIDATED_ROW_COUNT.value: "Validated expected count",
            cls.COPIED_PARQUET.value: "Copied parquet to storage account",
            cls.MOVED_JSON.value: "Moved JSON trigger file",
        }


class NonSSFStepStatus(BaseStepStatus):
    EXPECTED = 0
    RECEIVED = 1
    INIT_CHECKS = 2
    CONVERTED_PARQUET = 3
    MOVED_SRC = 4
    LOADED_STG = 5
    CHECKED_DQ = 6
    COMPLETED = 7
    REDELIVERY = 8

    @classmethod
    def _data_specific_descriptions(cls: type["NonSSFStepStatus"]) -> dict[int, str]:
        """Define data-specific descriptions for the Non-SSF pipeline."""
        return {
            cls.RECEIVED.value: "Received / Placed",
            cls.INIT_CHECKS.value: "Initial checks done",
            cls.CONVERTED_PARQUET.value: "Converted to Parquet",
            cls.MOVED_SRC.value: "Moved source file",
        }


class SSFStepStatus(BaseStepStatus):
    EXPECTED = 0
    EXTRACTED = 1
    VALIDATED_ROW_COUNT = 2
    EXPORTED_PARQUET = 3
    XSD_CONVERTED = 4
    LOADED_STG = 5
    CHECKED_DQ = 6
    COMPLETED = 7
    REDELIVERY = 8

    @classmethod
    def _data_specific_descriptions(cls: type["SSFStepStatus"]) -> dict[int, str]:
        """Define data-specific descriptions for the SSF pipeline."""
        return {
            cls.EXTRACTED.value: "Extracted data from Instagram view",
            cls.VALIDATED_ROW_COUNT.value: "Completeness checked",
            cls.EXPORTED_PARQUET.value: "Exported to Parquet",
            cls.XSD_CONVERTED.value: "XSD converted to latest version",
        }


StepStatusClassTypes = (
    type[DialStepStatus] | type[NonSSFStepStatus] | type[SSFStepStatus]
)
StepStatusInstanceTypes = DialStepStatus | NonSSFStepStatus | SSFStepStatus
