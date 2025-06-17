import logging
import logging.config
from pathlib import Path

import yaml

from src.config.constants import PROJECT_ROOT_DIRECTORY

APP_LOGGER_NAME = "betl_src_poc_logger"  # "BETL-SRC-POC"


def get_logger(
    log_name: str = APP_LOGGER_NAME, config_file: str = "src/config/logging.config.yaml"
) -> logging.Logger:
    with Path.open(PROJECT_ROOT_DIRECTORY / config_file) as f:
        config = yaml.safe_load(f)
        logging.config.dictConfig(config)
    return logging.getLogger(log_name)
