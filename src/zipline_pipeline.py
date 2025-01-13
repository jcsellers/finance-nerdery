import json
import logging
import sqlite3
from datetime import datetime

import pandas as pd


def load_config(config_path="config.json"):
    """
    Load configuration from a JSON file.

    Args:
        config_path (str): Path to the JSON configuration file.

    Returns:
        dict: Configuration dictionary.
    """
    try:
        with open(config_path, "r") as file:
            config = json.load(file)
            logging.info(f"Configuration loaded successfully from {config_path}")
            return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error parsing JSON configuration: {e}")


def transform_to_zipline(
    data: pd.DataFrame, config: dict, sid: int, source_type="default"
) -> pd.DataFrame:
    """
    Transforms raw financial data into a Zipline-compatible DataFrame.

    Args:
        data (pd.DataFrame): Raw financial data with columns ['date', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'split_factor'].
        config (dict): Configuration with keys 'date_range' (list of [start_date, end_date]).
        sid (int): Security identifier (unique integer for each symbol).
        source_type (str): The data source type (default: "default").

    Returns:
        pd.DataFrame: Transformed data with columns ['date', 'open', 'high', 'low', 'close', 'volume', 'sid'].
    """
    if not isinstance(data, pd.DataFrame):
        raise ValueError("Input data must be a Pandas DataFrame.")
    if not isinstance(config, dict):
        raise ValueError("Config must be a dictionary.")
    if not isinstance(sid, int):
        raise ValueError("SID must be an integer.")

    required_columns = ["date", "open", "high", "low", "close"]
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    for col, default in {"volume": 0, "dividends": 0.0, "split_factor": 1.0}.items():
        if col not in data.columns:
            data[col] = default

    try:
        data["date"] = pd.to_datetime(data["date"])
    except Exception as e:
        raise ValueError(f"Error converting 'date' column to datetime. Details: {e}")

    start_date, end_date = config.get("date_range", [None, None])
    if start_date:
        start_date = pd.Timestamp(start_date)
        data = data[data["date"] >= start_date]
    if end_date:
        end_date = pd.Timestamp(end_date)
        data = data[data["date"] <= end_date]

    data = data.rename(
        columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }
    )

    data["sid"] = sid
    zipline_columns = ["date", "open", "high", "low", "close", "volume", "sid"]
    data = data[zipline_columns]
    data = data.sort_values(by="date").reset_index(drop=True)

    return data
