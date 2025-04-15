import re
from pathlib import Path

import yaml

from src.config.constants import PROJECT_ROOT_DIRECTORY


def parse_yaml(yaml_path: str | Path, parameters: dict | None = None) -> dict:
    with Path.open(PROJECT_ROOT_DIRECTORY / "business_logic" / yaml_path) as yaml_file:
        original_yaml = yaml_file.read()
    # Triple quotes in string constants
    yaml_str = re.sub(
        r":\s*[\"\']([^\n\'\"]*)[\"\']\s*(\n|$)", r": '''\1'''\n", original_yaml
    )
    # Single quotes around null constants
    yaml_str = re.sub(
        r":\s*null\s*(\n|$)", r": 'null'\n", yaml_str, flags=re.IGNORECASE
    )
    # Replace all placeholders ( {{ }} ) with values from mapping parameters.
    if parameters:
        for parameter, value in parameters.items():
            yaml_str = re.sub(
                rf"{{{{\s*{parameter}\s*}}}}", str(value), yaml_str, flags=re.IGNORECASE
            )

    business_logic = yaml.safe_load(yaml_str)
    for key in business_logic["expressions"]:
        business_logic["expressions"][key] = str(business_logic["expressions"][key])

    return business_logic
