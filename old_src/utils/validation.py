import logging

import pandas as pd
from utils.logger import get_logger

# Initialize logger
logger = get_logger(__name__)


def validate_date_ranges(data, start_date, end_date):
    """
    Validate that the data's date range matches the expected range.

    Args:
        data (pd.DataFrame): DataFrame containing the data to validate.
        start_date (datetime): Expected start date.
        end_date (datetime): Expected end date.

    Returns:
        pd.DataFrame: The validated DataFrame.
    """
    logger.info("Validating date ranges.")

    # Ensure date column exists and is in datetime format
    if "date" not in data.columns:
        raise ValueError("The provided data does not contain a 'date' column.")

    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data.dropna(subset=["date"], inplace=True)

    if data["date"].min() < start_date or data["date"].max() > end_date:
        logger.warning(
            f"Date range validation failed: Data contains dates outside {start_date} to {end_date}."
        )
        # Optionally filter to only valid ranges
        data = data[(data["date"] >= start_date) & (data["date"] <= end_date)]

    logger.info("Date range validation passed.")
    return data


def validate_row_counts(data, expected_count):
    """
    Validate that the number of rows in the data matches the expected count.

    Args:
        data (pd.DataFrame): DataFrame containing the data to validate.
        expected_count (int): Expected number of rows.

    Returns:
        bool: True if the row count matches; False otherwise.
    """
    actual_count = len(data)
    if actual_count != expected_count:
        logger.warning(
            f"Row count validation failed: Expected {expected_count} rows, "
            f"but got {actual_count} rows."
        )
        return False

    logger.info("Row count validation passed.")
    return True
