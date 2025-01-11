import logging
import os
import sqlite3

import pandas as pd
from pandas import DataFrame

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def save_to_database(dataframe: DataFrame, database_path: str):
    """
    Saves a DataFrame to the SQLite database. It first deletes any existing data for the
    given ticker to prevent duplicates and then appends the new data.

    Parameters:
        dataframe (DataFrame): The pandas DataFrame containing the data to be saved.
        database_path (str): The file path of the SQLite database.
    """
    validate_dataframe(dataframe)  # Validate the DataFrame before saving

    ticker = dataframe["ticker"].iloc[0]

    if not os.path.exists(database_path):
        raise FileNotFoundError(f"Database file not found at: {database_path}")

    with sqlite3.connect(database_path) as conn:
        cursor = conn.cursor()
        logging.info(f"Connected to SQLite database at {database_path}")
        # Safely delete existing rows for the ticker
        cursor.execute("DELETE FROM data WHERE ticker = ?", (ticker,))
        conn.commit()
        # Save the DataFrame to the database
        dataframe.to_sql("data", conn, if_exists="append", index=False)
        logging.info(f"Data for ticker {ticker} saved to database.")


def validate_database_schema(database_path: str):
    """
    Validates that the SQLite database contains the required schema.

    Parameters:
        database_path (str): The file path of the SQLite database.
    """
    required_tables = {"data"}
    if not os.path.exists(database_path):
        raise FileNotFoundError(f"Database file not found at: {database_path}")

    with sqlite3.connect(database_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        existing_tables = {row[0] for row in cursor.fetchall()}

    missing_tables = required_tables - existing_tables
    if missing_tables:
        raise ValueError(
            f"Database is missing required tables: {', '.join(missing_tables)}"
        )
    logging.info(
        f"Database schema validation successful. Existing tables: {existing_tables}"
    )


def ingest_file(filepath: str, database_path="../data/output/aligned_data.db"):
    """
    Ingests a single CSV file into the database.

    Parameters:
        filepath (str): The path to the CSV file to ingest.
        database_path (str): The file path of the SQLite database.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    # Read the CSV file
    try:
        dataframe = pd.read_csv(filepath)
    except pd.errors.EmptyDataError:
        raise ValueError(f"File {filepath} is empty or invalid.")

    # Validate the database schema
    validate_database_schema(database_path)

    # Save the data to the database
    save_to_database(dataframe, database_path)
    logging.info(f"File {filepath} ingested into database {database_path}.")


def validate_dataframe(dataframe: DataFrame):
    """
    Validates the DataFrame to ensure it adheres to required standards.

    Parameters:
        dataframe (DataFrame): The pandas DataFrame to validate.

    Raises:
        ValueError: If the DataFrame is missing required columns or contains invalid data.
    """
    # Required columns for the database
    required_columns = {"ticker", "Date", "Open", "High", "Low", "Close", "Volume"}

    # Check for missing columns
    missing_columns = required_columns - set(dataframe.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Validate column data types
    if not pd.api.types.is_numeric_dtype(dataframe["Volume"]):
        raise ValueError("The 'Volume' column must be numeric.")
    if not pd.api.types.is_datetime64_any_dtype(dataframe["Date"]):
        raise ValueError("The 'Date' column must be in datetime format.")

    # Ensure there are no duplicate dates for a single ticker
    duplicates = dataframe.duplicated(subset=["ticker", "Date"])
    if duplicates.any():
        raise ValueError("Duplicate rows found for the same ticker and date.")

    logging.info("DataFrame validation successful.")
