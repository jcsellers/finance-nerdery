### sqlite_utils.py
import sqlite3

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


def save_to_sqlite(db_path, table_name, dataframe):
    """
    Save a DataFrame to an SQLite database and handle schema mismatches.

    Args:
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the table to save data to.
        dataframe (pd.DataFrame): DataFrame to be saved.
    """
    try:
        # Remove duplicate columns from the DataFrame
        dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]

        schema_map = {
            "yahoo_data": "CREATE TABLE IF NOT EXISTS yahoo_data (date TEXT, ticker TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, PRIMARY KEY (date, ticker))",
            "fred_data": "CREATE TABLE IF NOT EXISTS fred_data (date TEXT, ticker TEXT, value REAL, data_flag TEXT, PRIMARY KEY (date, ticker))",
            "synthetic_cash": "CREATE TABLE IF NOT EXISTS synthetic_cash (date TEXT, value REAL, PRIMARY KEY (date))",
            "synthetic_linear": "CREATE TABLE IF NOT EXISTS synthetic_linear (date TEXT, value REAL, PRIMARY KEY (date))",
        }

        with sqlite3.connect(db_path) as conn:
            # Create table if schema is defined
            if table_name in schema_map:
                conn.execute(schema_map[table_name])

            # Save the DataFrame
            dataframe.to_sql(table_name, conn, if_exists="replace", index=False)
            logger.info(f"Data saved to SQLite table '{table_name}' in '{db_path}'.")
    except Exception as e:
        logger.error(
            f"Error saving data to SQLite (table: {table_name}, db: {db_path}): {e}"
        )
        raise
