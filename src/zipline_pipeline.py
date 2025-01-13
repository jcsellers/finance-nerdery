import json
import os
import sqlite3

import pandas as pd


def load_config(config_path):
    """Load the configuration file."""
    with open(config_path, "r") as f:
        return json.load(f)


def generate_column_mapping(tickers):
    """Generate dynamic column mappings based on tickers."""
    column_mapping = {"('date', '')": "date"}
    for ticker in tickers:
        for field in ["open", "high", "low", "close", "volume"]:
            column_mapping[f"('{ticker.lower()}', '{field}')"] = field
    return column_mapping


def transform_to_zipline(data, config, sid, column_mapping):
    """Transforms raw data into a Zipline-compatible DataFrame."""
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

    # Add SID column
    data["sid"] = sid

    # Select and order columns
    zipline_columns = ["date", "open", "high", "low", "close", "volume", "sid"]
    data = data[zipline_columns]

    return data


def fetch_and_transform_data(
    database_path, table_name, config, column_mapping, output_dir, synthetic_data=None
):
    """Fetch data from the database or use synthetic data, apply transformations, and save as CSV."""
    if synthetic_data is not None:
        # Handle synthetic data
        transformed_data = transform_to_zipline(
            data=synthetic_data,
            config=config["date_ranges"],
            sid=1,
            column_mapping=column_mapping,
        )
        if output_dir:
            output_file = f"{output_dir}/transformed_synthetic_data.csv"
            transformed_data.to_csv(output_file, index=False)
            print(f"Transformed synthetic data saved to: {output_file}")
        return transformed_data

    # Ensure database_path and table_name are provided for database operations
    if database_path is None or table_name is None:
        raise ValueError(
            "Database path and table name must be provided for database operations."
        )

    # Connect to the SQLite database
    conn = sqlite3.connect(database_path)

    # Fetch schema for dynamic querying
    schema_query = f"PRAGMA table_info({table_name});"
    schema = pd.read_sql(schema_query, conn)
    print("Schema for table:", table_name)
    print(schema)

    # Generate dynamic query based on column mapping
    select_clause = ", ".join(
        [f'"{col}" AS {alias}' for col, alias in column_mapping.items()]
    )
    query = f"SELECT {select_clause} FROM {table_name};"
    data = pd.read_sql(query, conn)

    # Apply transformations
    transformed_data = transform_to_zipline(
        data=data, config=config["date_ranges"], sid=1, column_mapping=column_mapping
    )

    # Save transformed data to CSV
    output_file = f"{output_dir}/transformed_{table_name}.csv"
    transformed_data.to_csv(output_file, index=False)
    print(f"Transformed data saved to: {output_file}")

    conn.close()
    return transformed_data


# Ensure CONFIG_PATH is set for tests
def ensure_config_path():
    """Ensure CONFIG_PATH is set."""
    if "CONFIG_PATH" not in os.environ:
        os.environ["CONFIG_PATH"] = "config/config.json"


# Main execution block
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
