import logging
import sqlite3

import pandas as pd
import utils.logger as logger

# Configure logger
logger = logging.getLogger(__name__)


def create_connection(db_path):
    """
    Create a database connection to the SQLite database.

    Args:
        db_path (str): Path to the SQLite database.

    Returns:
        sqlite3.Connection: Database connection object.
    """
    try:
        conn = sqlite3.connect(db_path)
        logger.info(f"Successfully connected to the database: {db_path}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise


def execute_query(conn, query, params=None):
    """
    Execute a SQL query.

    Args:
        conn (sqlite3.Connection): Database connection object.
        query (str): SQL query to execute.
        params (tuple, optional): Query parameters.

    Returns:
        sqlite3.Cursor: Cursor object with the query results.
    """
    try:
        cur = conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        conn.commit()
        logger.info(f"Query executed successfully: {query}")
        return cur
    except sqlite3.Error as e:
        logger.error(f"Error executing query: {e}")
        raise


def save_to_db(df, table_name, db_path, if_exists="replace"):
    """
    Save a pandas DataFrame to an SQLite database.

    Args:
        df (pd.DataFrame): DataFrame to save.
        table_name (str): Name of the table to save the data.
        db_path (str): Path to the SQLite database.
        if_exists (str, optional): Action if the table already exists. Default is "replace".

    Returns:
        None
    """
    try:
        conn = create_connection(db_path)
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        conn.close()
        logger.info(f"Data saved to table '{table_name}' in the database at {db_path}")
    except sqlite3.Error as e:
        logger.error(f"Error saving data to database: {e}")
        raise
    except ValueError as ve:
        logger.error(f"ValueError while saving to database: {ve}")
        raise


import logging
import sqlite3

logger = logging.getLogger("sqlite_utils")


import sqlite3


def ensure_table_matches_data(db_path, table_name, df):
    """
    Ensures the database table schema matches the DataFrame structure.

    Args:
        db_path (str): Path to the SQLite database.
        table_name (str): Name of the table to validate.
        df (pandas.DataFrame): DataFrame to compare against the table schema.

    Raises:
        ValueError: If the DataFrame is empty or the table name is invalid.
    """
    if df.empty:
        raise ValueError(
            "DataFrame is empty. Cannot ensure schema for an empty DataFrame."
        )

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get existing columns in the database table
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}

    # Get columns from the DataFrame
    new_columns = set(df.columns)

    # Determine missing columns
    missing_columns = new_columns - existing_columns

    # Add missing columns to the table
    for col in missing_columns:
        column_type = "TEXT"  # Default type, adjust if specific types are needed
        if pd.api.types.is_integer_dtype(df[col]):
            column_type = "INTEGER"
        elif pd.api.types.is_float_dtype(df[col]):
            column_type = "REAL"
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            column_type = "TEXT"
        elif pd.api.types.is_bool_dtype(df[col]):
            column_type = "INTEGER"  # SQLite does not have a BOOLEAN type

        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {column_type}")
        conn.commit()

    conn.close()


def ensure_table_schema(db_path, table_name="daily_prices"):
    """
    Ensures the table schema exists in the SQLite database.

    Args:
        db_path (str): Path to the SQLite database.
        table_name (str): Name of the table to ensure schema.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        if table_name == "daily_prices":
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_prices (
                    date TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    symbol TEXT
                );
            """
            )
        logger.info(f"Schema ensured for table: {table_name}")
    except Exception as e:
        logger.error(f"Failed to ensure schema for table {table_name}: {e}")
    finally:
        conn.commit()
        conn.close()


def validate_date_ranges(df, start_date, end_date):
    """
    Filter rows in a DataFrame to ensure dates fall within the given range.

    Args:
        df (pd.DataFrame): Input DataFrame with a 'date' column.
        start_date (str): Start date in ISO format (YYYY-MM-DD).
        end_date (str): End date in ISO format (YYYY-MM-DD) or "current".

    Returns:
        pd.DataFrame: Filtered DataFrame with dates in the valid range.
    """
    if "date" not in df.columns:
        logger.warning("No 'date' column found in the DataFrame.")
        return df

    # Replace 'current' with today's date
    if end_date == "current":
        end_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    mask = (df["date"] >= pd.to_datetime(start_date)) & (
        df["date"] <= pd.to_datetime(end_date)
    )
    filtered_df = df.loc[mask]
    logger.info(f"Filtered data to date range: {start_date} to {end_date}")
    return filtered_df


def clean_column_names(df):
    """
    Clean column names by converting them to lowercase and replacing spaces with underscores.

    Args:
        df (pd.DataFrame): DataFrame whose column names need cleaning.

    Returns:
        pd.DataFrame: DataFrame with cleaned column names.
    """
    df.columns = (
        df.columns.str.lower()
        .str.replace(" ", "_")
        .str.replace("(", "")
        .str.replace(")", "")
    )
    logger.info("Column names cleaned successfully.")
    return df
