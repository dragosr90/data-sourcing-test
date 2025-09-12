import re
from pathlib import Path

import yaml

# MAPPING_ROOT_DIR = Path("/Workspace/Shared/deployment/mappings").resolve()

from abnamro_bsrc_etl.config.business_logic import BusinessLogicConfig
#from abnamro_bsrc_etl.config.constants import MAPPING_ROOT_DIR
from abnamro_bsrc_etl.utils.parameter_utils import standardize_delivery_entity


def parse_yaml(
    yaml_path: Path | str,
    parameters: dict | None = None,
    business_logic_path: Path | str = "/Workspace/Shared/deployment/mappings/business_logic"
) -> BusinessLogicConfig:
    """Parse YAML with business logic and replace placeholders with parameters.

    This function processes a YAML file by:
    1. Replacing quoted strings (single or double quotes) with triple single quotes
        ('''value''').
    2. Handling null constants by enclosing them in single quotes ('null').
    3. Replacing placeholders (e.g., {{ PARAMETER }}) with corresponding values from the
        `parameters` dictionary.
    4. Standardizing the value for the `DELIVERY_ENTITY` parameter if required.

    Args:
        yaml_path (str | Path): Path to the YAML file containing business logic.
        parameters (dict | None, optional): A dictionary of parameters to replace in the
            YAML.

    Returns:
        BusinessLogicConfig: Business Logic mapping for ETL
    """
    with Path.open(Path("/Workspace/Shared/deployment/mappings/business_logic", yaml_path)) as yaml_file:
        original_yaml = yaml_file.read()

    # Replacing quoted strings (single or double quotes) with triple single quotes
    yaml_str = re.sub(r"(['\"])([^'\"]*?)\1", r"'''\2'''", original_yaml)

    # Single quotes around null constants
    yaml_str = re.sub(
        r":\s*null\s*(\n|$)", r": 'null'\n", yaml_str, flags=re.IGNORECASE
    )
    # Replace all placeholders ( {{ }} ) with values from mapping parameters.
    if parameters:
        for parameter, value in parameters.items():
            if parameter == "DELIVERY_ENTITY":
                std_delivery_entity = standardize_delivery_entity(value)
                # Replace {{ DELIVERY_ENTITY }} based on context
                yaml_str = replace_delivery_entity(
                    yaml_str,
                    delivery_entity=value,
                    std_delivery_entity=std_delivery_entity,
                )
            else:
                # Default replacement logic for other parameters
                yaml_str = re.sub(
                    rf"{{{{\s*{parameter}\s*}}}}",
                    str(value),
                    yaml_str,
                    flags=re.IGNORECASE,
                )

    business_logic = yaml.safe_load(yaml_str)
    for key in business_logic["expressions"]:
        business_logic["expressions"][key] = str(business_logic["expressions"][key])

    return alias_hyphen_columns(business_logic)


def replace_delivery_entity(
    yaml_str: str, delivery_entity: str, std_delivery_entity: str
) -> str:
    """Replace {{ DELIVERY_ENTITY }} based on its context in the YAML string.

    Note:

     - The pattern r"(')?{{ DELIVERY_ENTITY }}(')?" matches the
        {{ DELIVERY_ENTITY }} placeholder with optional preceding and succeeding single
        quotes.
     - The lambda function checks if the match has quotes (m.group(1) and m.group(2))
        and determines whether to use delivery_entity (when quoted) or
        std_delivery_entity (when not quoted).

    Args:
        yaml_str (str): The YAML string.
        delivery_entity (str): The original delivery entity.
        std_delivery_entity (str): The standardized delivery entity.

    Returns:
        str: The updated YAML string with replacements applied.
    """
    return re.sub(
        r"(')?{{ DELIVERY_ENTITY }}(')?",
        lambda m: f"{m.group(1) or ''}{delivery_entity if m.group(1) else std_delivery_entity}{m.group(2) or ''}",  # noqa: E501
        yaml_str,
    )


def alias_hyphen_columns(
    business_logic: BusinessLogicConfig, replacement_character: str = "_"
) -> BusinessLogicConfig:
    """Replace hyphens (-) in column names with a specified replacement character.

    This function iterates through the `sources` section of the `business_logic` config
    and replaces hyphens in column names with the specified replacement character.
    If a column name contains a hyphen, it is transformed into a SQL alias format:
    "`original-column-name` as original_column_name".
    Columns without hyphens remain unchanged.

    Args:
        business_logic (BusinessLogicConfig): A dictionary-like configuration containing
            the `sources` section with column names to process.
        replacement_character (str, optional): The character to replace hyphens with.
            Defaults to "_".

    Returns:
        BusinessLogicConfig: The updated `business_logic` configuration with
            transformed column names.
    """
    for source in business_logic["sources"]:
        if source.get("columns"):
            source["columns"] = [
                f"`{col}` as {col.replace('-', replacement_character)}"
                if "-" in col
                else col
                for col in source["columns"]
            ]
    return business_logic
