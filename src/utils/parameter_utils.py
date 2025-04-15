import re
from typing import Optional, Tuple

from src.utils.logging_util import get_logger

logger = get_logger()


def standardize_delivery_entity(delivery_entity_value: Optional[str]) -> Optional[str]:
    """
    Standardizes delivery entity to lowercase without special characters.

    This function transforms the delivery entity values to a consistent format
    (lowercase with no special characters) that the Run_YAML_mapping_minimal
    expects. It allows users to input delivery entities in their original format
    while ensuring compatibility with the expected standardized values (e.g. "ihubfr1").

    Args:
        delivery_entity_value: The delivery entity as a string or None.

    Returns:
        Optional[str]: Standardized delivery entity (lowercase, no special characters)
                      or None if input is None.

    Examples:
        >>> standardize_delivery_entity("IHUB-FR1")
        "ihubfr1"
        >>> standardize_delivery_entity("ihubfr1")
        "ihubfr1"
        >>> standardize_delivery_entity("IHUB_FR1")
        "ihubfr1"
    """
    # Handle None case
    if delivery_entity_value is None:
        return None

    # Handle empty string
    if delivery_entity_value == "":
        return ""

    # Convert non-string values to strings
    if not isinstance(delivery_entity_value, str):
        error_type_msg = (
            "Delivery entity value must be a string or a simple type, "
            f"got {type(delivery_entity_value)}"
        )
        raise TypeError(error_type_msg)

    # Extract original value for logging
    original_value = delivery_entity_value

    # Remove special characters (hyphens, spaces, underscores, periods)
    standardized = re.sub(r"[^a-zA-Z0-9]", "", delivery_entity_value).lower()

    # Log the transformation if it changed
    if standardized != original_value:
        logger.info(
            f"Standardized delivery entity '{original_value}' to '{standardized}'"
        )

    return standardized


def parse_delivery_entity(
    delivery_entity_value: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Parses a delivery entity and returns both the original and standardized forms.

    This function is useful when you need to maintain both the original format
    (for display or lineage purposes) and the standardized format
    (for internal processing).

    Args:
        delivery_entity_value: The delivery entity as a string or None.

    Returns:
        Tuple[Optional[str], Optional[str]]: A tuple containing
        (original_value, standardized_value)

    Examples:
        >>> parse_delivery_entity("IHUB-FR1")
        ('IHUB-FR1', 'ihubfr1')
    """

    # Store original value
    original_value = delivery_entity_value

    # Get standardized value
    standardized_value = standardize_delivery_entity(delivery_entity_value)

    return original_value, standardized_value


def is_standard_format(delivery_entity_value: Optional[str]) -> bool:
    """
    Checks if a delivery entity is in the standard format.

    Standard format is defined as lowercase with no special characters.

    Args:
        delivery_entity_value: The delivery entity to check.

    Returns:
        bool: True if the value is already in standard format, False otherwise.

    Examples:
        >>> is_standard_format("ihubfr1")
        True
        >>> is_standard_format("IHUB-FR1")
        False
    """
    if delivery_entity_value is None:
        return True

    if not isinstance(delivery_entity_value, str):
        error_type_msg = (
            "Delivery entity value must be a string or None, "
            f"got {type(delivery_entity_value)}"
        )
        raise TypeError(error_type_msg)
    # Check if already standardized (lowercase with no special characters)
    standardized = standardize_delivery_entity(delivery_entity_value)
    return delivery_entity_value == standardized
