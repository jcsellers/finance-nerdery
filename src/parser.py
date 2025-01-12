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
    """
    Parse and validate the configuration file.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        dict: Parsed and validated configuration.

    Raises:
        FileNotFoundError: If the config file does not exist.
        JSONDecodeError: If the config file is not valid JSON.
        ValueError: If any validation fails.
    """
    try:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

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
    except FileNotFoundError as fnf_error:
        logger.error(f"File not found: {fnf_error}")
        raise
    except json.JSONDecodeError as json_error:
        logger.error(f"Invalid JSON in configuration file: {json_error}")
        raise
    except ValueError as val_error:
        logger.error(f"Validation error: {val_error}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error parsing configuration: {e}")
        raise
