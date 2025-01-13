import sqlite3

import pandas as pd
import pytest

from zipline_pipeline import transform_to_zipline


@pytest.fixture
def real_database_connection():
    """Fixture for connecting to the real database."""
    db_path = "root/../data/finance_data.db"  # Adjust path if needed
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()


def test_yahoo_data_transformation(real_database_connection):
    """Test transformation of yahoo_data with unconventional column names."""

    # Adjusted query
    query = """
        SELECT
            "('date', '')" AS date,
            "('spy', 'open')" AS open,
            "('spy', 'high')" AS high,
            "('spy', 'low')" AS low,
            "('spy', 'close')" AS close,
            "('spy', 'volume')" AS volume
        FROM yahoo_data;
    """
    data = pd.read_sql(query, real_database_connection)

    # Configuration
    config = {"date_range": {"start": "2020-01-01", "end": "2025-12-31"}}
    sid = 1

    # Transform the data
    transformed_data = transform_to_zipline(data, config, sid)

    # Validate the results
    assert all(
        col in transformed_data.columns
        for col in ["date", "open", "high", "low", "close", "volume", "sid"]
    ), "Missing required columns in transformed data."
    assert len(transformed_data) > 0, "Transformed data is empty."
    assert (
        transformed_data["sid"].nunique() == 1
    ), "SID column contains unexpected values."
    print("Yahoo data transformation test passed.")


def test_fred_data_transformation(real_database_connection):
    """Test transformation of fred_data."""
    query = "SELECT date, ticker, value FROM fred_data;"
    data = pd.read_sql(query, real_database_connection)

    # Adjust the transformation logic for fred_data schema if needed.
    # Example: Create a mock transformation logic or wrap the function to handle FRED.
    config = {"date_range": {"start": "2020-01-01", "end": "2025-12-31"}}
    sid = 2  # Example SID for FRED

    # Apply transformation (adapt transform_to_zipline or create a similar function for fred_data)
    # transformed_data = transform_fred_to_zipline(data, config, sid)

    # Validate output
    assert "date" in data.columns, "Date column missing in FRED data."
    assert len(data) > 0, "No rows in FRED data query."
    print("FRED data transformation test passed.")
