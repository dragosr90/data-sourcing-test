import pytest

from src.utils.parameter_utils import (
    is_standard_format,
    parse_delivery_entity,
    standardize_delivery_entity,
)


# Parametrized test cases for standardize_delivery_entity
@pytest.mark.parametrize(
    ("original", "expected"),
    [
        # Basic transformation cases
        ("IHUB-FR1", "ihubfr1"),  # Uppercase with hyphen
        ("ihubfr1", "ihubfr1"),  # Already standardized
        ("IHUB FR1", "ihubfr1"),  # With space
        ("ihub-fr1", "ihubfr1"),  # Lowercase with hyphen
        # Complex cases
        ("IHUB--FR1", "ihubfr1"),  # Multiple hyphens
        ("IHUB FR-1", "ihubfr1"),  # Mixed spaces and hyphens
        ("IHUB_FR1", "ihubfr1"),  # With underscore
        ("IHUB.FR1", "ihubfr1"),  # With period
        ("IHUB-FR-1", "ihubfr1"),  # Multiple segments
        # Additional special characters cases
        ("IHUB@FR#1", "ihubfr1"),  # With @ and # symbols
        ("IHUB/FR\\1", "ihubfr1"),  # With slashes
        ("IHUB(FR)1", "ihubfr1"),  # With parentheses
        ("IHUB:FR;1", "ihubfr1"),  # With semicolon and colon
        ("IHUB!FR?1", "ihubfr1"),  # With exclamation and question marks
        ("IHUB+FR=1", "ihubfr1"),  # With math symbols
        ("IHUB$FR%1", "ihubfr1"),  # With currency symbols
        ("IHUB&FR|1", "ihubfr1"),  # With ampersand and pipe
        # Different region examples
        ("EMEA-UK1", "emeauk1"),  # Europe region
        ("APAC_JP2", "apacjp2"),  # Asia Pacific with underscore
        ("NA.US3", "naus3"),  # North America with period
        ("LAT-BR_4", "latbr4"),  # Latin America with mixed separators
        # Edge cases
        ("", ""),  # Empty string
        (None, None),  # None value
        ("123-456", "123456"),  # Numeric with hyphen
        ("A-B-C", "abc"),  # Simple letters with hyphens
    ],
)
def test_standardize_delivery_entity(original, expected):
    """Test that delivery entity values are properly standardized."""
    result = standardize_delivery_entity(original)
    msg = f"Standardize failed '{original}'. Got '{result}', expected '{expected}'"
    assert result == expected, msg


# Test with non-string inputs should now raise TypeError
@pytest.mark.parametrize(
    "input_value",
    [
        123,  # Integer
        45.67,  # Float
        True,  # Boolean
        [1, 2, 3],  # List
        {"key": "value"},  # Dictionary
        {1, 2, 3},  # Set
    ],
)
def test_standardize_delivery_entity_type_error(input_value):
    """Test taht TypeError is raised for non-string inputs."""
    with pytest.raises(TypeError):
        standardize_delivery_entity(input_value)


# Test logging behavior
def test_standardize_delivery_entity_logging(mocker):
    """Test that the function logs appropriately when transformation occurs."""
    # Mock the logger
    mock_logger = mocker.patch("src.utils.parameter_utils.logger")

    # Case 1: Value requires transformation
    standardize_delivery_entity("IHUB-FR1")
    mock_logger.info.assert_called_once_with(
        "Standardized delivery entity 'IHUB-FR1' to 'ihubfr1'"
    )

    # Case 2: Value already standardized
    mock_logger.reset_mock()
    standardize_delivery_entity("ihubfr1")
    mock_logger.info.assert_not_called()

    # Case 3: Empty value (no transformation needed)
    mock_logger.reset_mock()
    standardize_delivery_entity("")
    mock_logger.info.assert_not_called()


# Test parse_delivery_entity function
@pytest.mark.parametrize(
    ("input_value", "expected_original", "expected_standardized"),
    [
        ("IHUB-FR1", "IHUB-FR1", "ihubfr1"),
        ("ihubfr1", "ihubfr1", "ihubfr1"),
        (None, None, None),
    ],
)
def test_parse_delivery_entity(input_value, expected_original, expected_standardized):
    """Test that parse_delivery_entity returns correct values."""
    original, standardized = parse_delivery_entity(input_value)
    orig_msg = f"Original incorrect. Got {original}, expected {expected_original}"
    assert original == expected_original, orig_msg

    std_msg = (
        f"Standardized wrong. Got {standardized}, expected {expected_standardized}"
    )
    assert standardized == expected_standardized, std_msg


def test_parese_delivery_entity_type_error():
    """Test that parse_delivery_entity raises TypeError for non-string inputs"""
    with pytest.raises(TypeError):
        parse_delivery_entity(123)


# Test is_standard_format function
@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        ("ihubfr1", True),  # Already standard
        ("IHUB-FR1", False),  # Needs standardization
        ("ihub-fr1", False),  # Lowercase but has hyphen
        ("IHUBFR1", False),  # Uppercase without special chars
        ("", True),  # Empty string is considered standard
        (None, True),  # None is considered standard (edge case)
    ],
)
def test_is_standard_format(input_value, expected):
    """Test that is_standard_format identifies standardized values."""
    result = is_standard_format(input_value)
    msg = f"Format check for '{input_value}'. Got {result}, expected {expected}"
    assert result == expected, msg


# Test non-string inputs for is_standard_format
def test_is_standard_format_type_error():
    """Test that is_standard_format raises TypeError for non-string inputs."""
    with pytest.raises(TypeError):
        is_standard_format(123)
