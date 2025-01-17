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
