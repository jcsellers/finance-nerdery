import os
from datetime import datetime

from src.utils.logger import get_logger

logger = get_logger(__name__)


def validate_tickers(tickers):
    if not tickers or not isinstance(tickers, dict):
        logger.error("Invalid tickers configuration: must be a non-empty dictionary.")
        raise ValueError("Tickers must be a non-empty dictionary.")


def validate_aliases(aliases):
    seen = set()
    for source, ticker_aliases in aliases.items():
        if isinstance(ticker_aliases, dict):
            for ticker, alias in ticker_aliases.items():
                if alias in seen:
                    raise ValueError(f"Duplicate alias found: {alias}")
                seen.add(alias)
        elif isinstance(ticker_aliases, str):  # Handle flat dictionary structure
            if ticker_aliases in seen:
                raise ValueError(f"Duplicate alias found: {ticker_aliases}")
            seen.add(ticker_aliases)
        else:
            raise ValueError(f"Invalid alias format for source {source}.")


def validate_date_ranges(date_ranges):
    start_date = date_ranges.get("start_date")
    end_date = date_ranges.get("end_date")
    try:
        if start_date:
            datetime.strptime(start_date, "%Y-%m-%d")
        if end_date and end_date != "current":
            datetime.strptime(end_date, "%Y-%m-%d")
        if (
            start_date
            and end_date
            and start_date != "current"
            and end_date != "current"
        ):
            if datetime.strptime(start_date, "%Y-%m-%d") > datetime.strptime(
                end_date, "%Y-%m-%d"
            ):
                logger.error("Invalid date range: start_date cannot be after end_date.")
                raise ValueError("start_date cannot be after end_date.")
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        raise


def validate_paths(storage):
    for path_name, path_value in storage.items():
        if not os.path.exists(path_value):
            logger.warning(f"Path {path_value} does not exist. Creating it.")
            os.makedirs(path_value, exist_ok=True)
