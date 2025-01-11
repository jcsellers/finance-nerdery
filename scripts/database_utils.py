import sqlite3
import os
from pandas import DataFrame

def save_to_database(dataframe: DataFrame, database_path: str):
    """
    Saves a DataFrame to the SQLite database. It first deletes any existing data for the
    given ticker to prevent duplicates and then appends the new data.

    Parameters:
        dataframe (DataFrame): The pandas DataFrame containing the data to be saved.
        database_path (str): The file path of the SQLite database.
    """
    if "ticker" not in dataframe.columns:
        raise ValueError("The DataFrame must contain a 'ticker' column.")

    ticker = dataframe["ticker"].iloc[0]

    if not os.path.exists(database_path):
        raise FileNotFoundError(f"Database file not found at: {database_path}")

    with sqlite3.connect(database_path) as conn:
        cursor = conn.cursor()
        # Safely delete existing rows for the ticker
        cursor.execute("DELETE FROM prices WHERE ticker = ?", (ticker,))
        conn.commit()
        # Save the DataFrame to the database
        dataframe.to_sql("prices", conn, if_exists="append", index=False)

def validate_database_schema(database_path: str):
    """
    Validates that the SQLite database contains the required schema.

    Parameters:
        database_path (str): The file path of the SQLite database.
    """
    required_tables = {"prices"}
    if not os.path.exists(database_path):
        raise FileNotFoundError(f"Database file not found at: {database_path}")

    with sqlite3.connect(database_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        existing_tables = {row[0] for row in cursor.fetchall()}

    missing_tables = required_tables - existing_tables
    if missing_tables:
        raise ValueError(f"Database is missing required tables: {', '.join(missing_tables)}")

def fetch_data_from_database(database_path: str, query: str):
    """
    Fetches data from the SQLite database using a custom SQL query.

    Parameters:
        database_path (str): The file path of the SQLite database.
        query (str): The SQL query to execute.

    Returns:
        DataFrame: A pandas DataFrame containing the query results.
    """
    if not os.path.exists(database_path):
        raise FileNotFoundError(f"Database file not found at: {database_path}")

    with sqlite3.connect(database_path) as conn:
        return DataFrame.from_records(conn.execute(query).fetchall())
