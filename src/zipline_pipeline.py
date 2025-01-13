import json
import logging
import os
import sqlite3

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_config(config_path: str) -> dict:
    """
    Load the configuration file.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        dict: Parsed configuration data.
    """
    with open(config_path, "r") as f:
        return json.load(f)


def generate_column_mapping(tickers: list) -> dict:
    """
    Generate dynamic column mappings based on tickers.

    Args:
        tickers (list): List of tickers to generate mappings for.

    Returns:
        dict: Mapping from unconventional column names to standardized names.
    """
    column_mapping = {"('date', '')": "date"}
    for ticker in tickers:
        for field in ["open", "high", "low", "close", "volume"]:
            column_mapping[f"('{ticker.lower()}', '{field}')"] = field
    return column_mapping


def transform_to_zipline(
    data: pd.DataFrame, config: dict, sid: int, column_mapping: dict
) -> pd.DataFrame:
    """
    Transforms raw financial data into a Zipline-compatible DataFrame.

    Args:
        data (pd.DataFrame): Raw financial data with columns to be renamed.
        config (dict): Configuration dictionary containing 'start_date' and 'end_date'.
        sid (int): Security identifier for the dataset.
        column_mapping (dict): Mapping from raw column names to standard column names.

    Returns:
        pd.DataFrame: Transformed data with columns ['date', 'open', 'high', 'low', 'close', 'volume', 'sid'].
    """
    # Rename columns using the mapping
    data = data.rename(columns=column_mapping)

    # Validate required columns
    required_columns = ["date", "open", "high", "low", "close", "volume"]
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Convert date column to datetime
    try:
        data["date"] = pd.to_datetime(data["date"])
    except Exception as e:
        raise ValueError(f"Error converting 'date' column to datetime. Details: {e}")

    # Apply date range filtering
    date_range = config
    start_date = (
        pd.Timestamp(date_range.get("start_date", None))
        if "start_date" in date_range
        else None
    )
    end_date = (
        pd.Timestamp(date_range.get("end_date", None))
        if "end_date" in date_range and date_range["end_date"] != "current"
        else pd.Timestamp.now()
    )
    if start_date:
        data = data[data["date"] >= start_date]
    if end_date:
        data = data[data["date"] <= end_date]

    if data.empty:
        logging.warning("No data matched the specified date range.")

    # Add SID column
    data["sid"] = sid

    # Select and order columns
    zipline_columns = ["date", "open", "high", "low", "close", "volume", "sid"]
    data = data[zipline_columns]

    logging.info("Transformation to Zipline-compatible format complete.")
    return data


def fetch_and_transform_data(
    database_path: str,
    table_name: str,
    config: dict,
    column_mapping: dict,
    output_dir: str,
    synthetic_data: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Fetch data from the database or use synthetic data, apply transformations, and save as CSV.

    Args:
        database_path (str): Path to the SQLite database file.
        table_name (str): Name of the table to query.
        config (dict): Configuration dictionary containing date ranges and other settings.
        column_mapping (dict): Mapping from raw column names to standard column names.
        output_dir (str): Directory to save the output CSV file.
        synthetic_data (pd.DataFrame, optional): Synthetic data to be transformed instead of fetching from the database.

    Returns:
        pd.DataFrame: Transformed data.
    """
    if synthetic_data is not None:
        # Handle synthetic data
        logging.info("Processing synthetic data.")
        transformed_data = transform_to_zipline(
            data=synthetic_data,
            config=config["date_ranges"],
            sid=1,
            column_mapping=column_mapping,
        )
        if output_dir:
            output_file = f"{output_dir}/transformed_synthetic_data.csv"
            transformed_data.to_csv(output_file, index=False)
            logging.info(f"Transformed synthetic data saved to: {output_file}")
        return transformed_data

    # Ensure database_path and table_name are provided for database operations
    if database_path is None or table_name is None:
        raise ValueError(
            "Database path and table name must be provided for database operations."
        )

    # Connect to the SQLite database
    logging.info(f"Connecting to database at {database_path}.")
    conn = sqlite3.connect(database_path)

    # Fetch schema for dynamic querying
    schema_query = f"PRAGMA table_info({table_name});"
    schema = pd.read_sql(schema_query, conn)
    logging.info(f"Schema for table {table_name}:\n{schema}")

    # Generate dynamic query based on column mapping
    select_clause = ", ".join(
        [f'"{col}" AS {alias}' for col, alias in column_mapping.items()]
    )
    query = f"SELECT {select_clause} FROM {table_name};"
    data = pd.read_sql(query, conn)
    logging.info("Data fetched successfully from the database.")

    # Apply transformations
    transformed_data = transform_to_zipline(
        data=data, config=config["date_ranges"], sid=1, column_mapping=column_mapping
    )

    # Save transformed data to CSV
    output_file = f"{output_dir}/transformed_{table_name}.csv"
    transformed_data.to_csv(output_file, index=False)
    logging.info(f"Transformed data saved to: {output_file}")

    conn.close()
    return transformed_data


def ensure_config_path() -> None:
    """
    Ensure CONFIG_PATH is set.

    If CONFIG_PATH is not already set as an environment variable, it defaults to 'config/config.json'.
    """
    if "CONFIG_PATH" not in os.environ:
        os.environ["CONFIG_PATH"] = "config/config.json"


if __name__ == "__main__":
    ensure_config_path()
    config_path = os.getenv("CONFIG_PATH")
    config = load_config(config_path)

    # Extract settings
    sqlite_path = config["storage"]["SQLite"]
    csv_output_dir = config["storage"]["CSV"]
    tickers = config["tickers"]["Yahoo Finance"]

    # Generate column mapping
    COLUMN_MAPPING = generate_column_mapping(tickers)

    # Run pipeline for yahoo_data table
    fetch_and_transform_data(
        database_path=sqlite_path,
        table_name="yahoo_data",
        config=config,
        column_mapping=COLUMN_MAPPING,
        output_dir=csv_output_dir,
    )
