import logging
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)


def validate_row_counts(dataframe, db_path, table_name, csv_path):
    """
    Validate row counts across DataFrame, SQLite, and CSV.

    Args:
        dataframe (pd.DataFrame): DataFrame to validate.
        db_path (str): Path to SQLite database.
        table_name (str): Name of the SQLite table.
        csv_path (str): Path to the CSV file.

    Returns:
        bool: True if row counts match, False otherwise.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            sqlite_count = conn.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]
        csv_count = len(pd.read_csv(csv_path))

        logger.info(
            f"Row counts: DataFrame={len(dataframe)}, SQLite={sqlite_count}, CSV={csv_count}"
        )

        if len(dataframe) != sqlite_count or len(dataframe) != csv_count:
            logger.error(
                f"Row count mismatch: DataFrame={len(dataframe)}, "
                f"SQLite={sqlite_count}, CSV={csv_count}"
            )
            return False
        return True
    except Exception as e:
        logger.error(f"Validation failed for table '{table_name}': {e}")
        raise


def save_and_validate_pipeline_data(dataframe, db_path, table_name, csv_path):
    """
    Save pipeline data to SQLite and CSV, then validate row counts.

    Args:
        dataframe (pd.DataFrame): DataFrame to save and validate.
        db_path (str): Path to SQLite database.
        table_name (str): SQLite table name.
        csv_path (str): Path to the CSV file.
    """
    try:
        # Save to SQLite
        with sqlite3.connect(db_path) as conn:
            dataframe.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.info(f"Data saved to SQLite table '{table_name}'")

        # Save to CSV
        dataframe.to_csv(csv_path, index=False)
        logger.info(f"Data saved to CSV at '{csv_path}'")

        # Validate row counts
        if not validate_row_counts(dataframe, db_path, table_name, csv_path):
            logger.warning(f"Row count validation failed for table '{table_name}'")
    except Exception as e:
        logger.error(f"Error in save_and_validate_pipeline_data: {e}")
        raise
