import os
import sqlite3
import tempfile

import pandas as pd
import pytest
from src.zipline_pipeline import (
    fetch_and_transform_data,
    generate_column_mapping,
    load_config,
    transform_to_zipline,
)


@pytest.fixture
def temporary_database():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_db:
        conn = sqlite3.connect(temp_db.name)

        # Create a sample table and insert test data
        conn.execute(
            "CREATE TABLE yahoo_data (date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER)"
        )
        conn.executemany(
            "INSERT INTO yahoo_data (date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("2025-01-01", 100.0, 105.0, 95.0, 102.0, 1000),
                ("2025-01-02", 102.0, 107.0, 97.0, 104.0, 1100),
            ],
        )
        conn.commit()
        conn.close()

        yield temp_db.name

    # Retry cleanup to handle slow connection release
    for _ in range(3):
        try:
            os.remove(temp_db.name)
            break
        except PermissionError:
            import time

            time.sleep(0.1)


def test_transform_to_zipline_with_temp_db(temporary_database):
    """Test transformation using a temporary SQLite database."""
    conn = sqlite3.connect(temporary_database)

    # Fetch and transform data
    query = "SELECT * FROM yahoo_data;"
    data = pd.read_sql(query, conn)
    conn.close()

    column_mapping = {
        "date": "date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
    }

    config = {"date_ranges": {"start_date": "2025-01-01", "end_date": "2025-01-02"}}

    transformed_data = transform_to_zipline(
        data, config["date_ranges"], sid=1, column_mapping=column_mapping
    )

    # Validate transformed data
    assert not transformed_data.empty, "Transformed data should not be empty."
    assert len(transformed_data) == 2, "Expected 2 rows in the transformed data."
    assert (
        "date" in transformed_data.columns
    ), "Missing 'date' column in transformed data."


def test_fetch_and_transform_with_temp_db(temporary_database):
    """Test the fetch_and_transform_data function with a temporary SQLite database."""
    output_dir = tempfile.mkdtemp()

    column_mapping = {
        "date": "date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
    }

    config = {
        "date_ranges": {"start_date": "2025-01-01", "end_date": "2025-12-31"},
        "storage": {"SQLite": temporary_database, "CSV": output_dir},
        "tickers": {"Yahoo Finance": ["SPY"]},
    }

    fetch_and_transform_data(
        database_path=temporary_database,
        table_name="yahoo_data",
        config=config,
        column_mapping=column_mapping,
        output_dir=output_dir,
    )

    # Validate output file
    output_file = os.path.join(output_dir, "transformed_yahoo_data.csv")
    assert os.path.exists(output_file), "Output file was not created."

    output_data = pd.read_csv(output_file)
    assert not output_data.empty, "Output CSV should not be empty."
    assert "open" in output_data.columns, "Missing 'open' column in output CSV."

    # Cleanup
    os.remove(output_file)
    os.rmdir(output_dir)
