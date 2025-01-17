import os
import sqlite3

import pandas as pd
import pytest
from src.utils.row_count_validation import (
    save_and_validate_pipeline_data,
    validate_row_counts,
)


def test_validate_row_counts(tmp_path):
    """
    Test the validate_row_counts function.
    """
    # Setup temporary SQLite database and CSV path
    db_path = os.path.join(tmp_path, "test.db")
    csv_path = os.path.join(tmp_path, "test_data.csv")
    table_name = "test_table"

    # Create test data
    data = pd.DataFrame(
        {
            "date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "value": [1.0, 2.0, 3.0],
        }
    )

    # Save data to SQLite and CSV
    with sqlite3.connect(db_path) as conn:
        data.to_sql(table_name, conn, if_exists="replace", index=False)
    data.to_csv(csv_path, index=False)

    # Validate row counts
    assert validate_row_counts(
        data, db_path, table_name, csv_path
    ), "Row counts should match."


def test_save_and_validate_pipeline_data(tmp_path):
    """
    Test the save_and_validate_pipeline_data function.
    """
    # Setup temporary SQLite database and CSV path
    db_path = os.path.join(tmp_path, "pipeline_test.db")
    csv_path = os.path.join(tmp_path, "pipeline_test_data.csv")
    table_name = "pipeline_test_table"

    # Create test data
    data = pd.DataFrame(
        {
            "date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "value": [10.0, 20.0, 30.0],
        }
    )

    # Call save_and_validate_pipeline_data
    save_and_validate_pipeline_data(data, db_path, table_name, csv_path)

    # Validate SQLite data
    with sqlite3.connect(db_path) as conn:
        sqlite_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    assert sqlite_count == len(data), "SQLite row count should match DataFrame."

    # Validate CSV data
    csv_data = pd.read_csv(csv_path)
    assert len(csv_data) == len(data), "CSV row count should match DataFrame."
