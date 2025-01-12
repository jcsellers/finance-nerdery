import json
import os

from src.utils.logger import get_logger
from src.utils.validation import (
    validate_aliases,
    validate_date_ranges,
    validate_paths,
    validate_tickers,
)

logger = get_logger(__name__)


def parse_config(config_path):
    try:
        with open(config_path, "r") as file:
            config = json.load(file)
        logger.info("Configuration file loaded successfully.")

        # Validate configuration
        validate_tickers(config.get("tickers", {}))
        validate_aliases(config.get("aliases", {}))
        validate_date_ranges(config.get("date_ranges", {}))
        validate_paths(config.get("storage", {}))

        logger.info("Configuration validation completed successfully.")
        return config
    except Exception as e:
        logger.error(f"Error parsing configuration: {e}")
        raise
