from dataclasses import dataclass

from src.staging.status import DialStepStatus, NonSSFStepStatus, SSFStepStatus


@dataclass
class BaseExtractionError(Exception):
    """
    Base class for extraction errors with common functionality.

    Attributes:
        step_status: The step status associated with the error.
        additional_info: Additional information about the error.
    """

    step_status: DialStepStatus | NonSSFStepStatus | SSFStepStatus
    additional_info: str = ""

    def __post_init__(self) -> None:
        super().__init__(self._generate_message())

    def _generate_message(self) -> str:
        return self.fail_message()

    def fail_message(self) -> str:
        """Generate the failure message for the error.

        Returns:
            str: The failure message.
        """
        base_message = f"Step '{self.step_status.get_description()}' failed."
        additional_info = f" {self.additional_info}" if self.additional_info else ""
        return f"{base_message}{additional_info}"


@dataclass
class DialExtractionError(BaseExtractionError):
    step_status: DialStepStatus


@dataclass
class NonSSFExtractionError(BaseExtractionError):
    step_status: NonSSFStepStatus


@dataclass
class SSFExtractionError(BaseExtractionError):
    step_status: SSFStepStatus
