import sqlite3
import time

import numpy as np
import pandas as pd
import pytest

from zipline_pipeline import transform_to_zipline


@pytest.fixture
def in_memory_db():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
    CREATE TABLE complete_data (
        date TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL NOT NULL,
        volume INTEGER DEFAULT 0,
        dividends REAL DEFAULT 0.0,
        split_factor REAL DEFAULT 1.0
    );
    """
    )
    conn.executemany(
        """
    INSERT INTO complete_data (date, open, high, low, close, volume, dividends, split_factor)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """,
        [
            ("2025-01-01", 100.0, 101.0, 99.0, 100.0, 1000, 0.0, 1.0),
            ("2025-01-02", 102.0, 103.0, 101.0, 102.0, 1500, 0.0, 1.0),
        ],
    )
    yield conn
    conn.close()


def generate_large_dataset(num_rows=10000):
    """Generate a synthetic dataset with the specified number of rows."""
    dates = pd.date_range(start="2025-01-01", periods=num_rows, freq="D")
    data = {
        "date": np.random.choice(dates, size=num_rows),
        "open": np.random.uniform(100, 200, size=num_rows),
        "high": np.random.uniform(200, 300, size=num_rows),
        "low": np.random.uniform(50, 100, size=num_rows),
        "close": np.random.uniform(150, 250, size=num_rows),
        "volume": np.random.randint(1000, 10000, size=num_rows),
    }
    return pd.DataFrame(data)


def test_integration_with_sqlite(in_memory_db):
    query = "SELECT * FROM complete_data;"
    data = pd.read_sql(query, in_memory_db)

    config = {"date_range": {"start": "2025-01-01", "end": "2025-12-31"}}
    result = transform_to_zipline(data, config, sid=1)

    date_range = pd.date_range(start="2025-01-01", end="2025-12-31")
    assert all(
        col in result.columns
        for col in ["date", "open", "high", "low", "close", "volume", "sid"]
    ), "Missing required columns in output."
    assert len(result) > 0, "No data in result."
    assert result["date"].isin(date_range).all(), "Dates are outside the range."


def test_missing_columns():
    data_missing_cols = pd.DataFrame({"date": ["2025-01-01"], "close": [105]})
    config = {"date_range": {"start": "2025-01-01", "end": "2025-12-31"}}
    with pytest.raises(ValueError, match="Missing required columns"):
        transform_to_zipline(data_missing_cols, config, sid=1)


def test_invalid_date():
    data_invalid_date = pd.DataFrame(
        {
            "date": ["invalid_date"],
            "open": [100],
            "high": [110],
            "low": [90],
            "close": [105],
            "volume": [1000],
        }
    )
    config = {"date_range": {"start": "2025-01-01", "end": "2025-12-31"}}
    with pytest.raises(ValueError, match="Error converting 'date' column to datetime"):
        transform_to_zipline(data_invalid_date, config, sid=1)


def test_invalid_sid():
    data_valid = pd.DataFrame(
        {
            "date": ["2025-01-01"],
            "open": [100],
            "high": [110],
            "low": [90],
            "close": [105],
            "volume": [1000],
        }
    )
    config = {"date_range": {"start": "2025-01-01", "end": "2025-12-31"}}
    with pytest.raises(ValueError, match="SID must be an integer"):
        transform_to_zipline(data_valid, config, sid="invalid")


def test_transform_performance():
    """Test the performance of transform_to_zipline with a large dataset."""
    # Generate a dataset with 10,000 rows
    large_data = generate_large_dataset(10000)

    # Define a test configuration
    config = {"date_range": {"start": "2025-01-01", "end": "2025-12-31"}}

    # Measure the time taken
    start_time = time.time()
    result = transform_to_zipline(large_data, config, sid=1)
    elapsed_time = time.time() - start_time

    # Assert the function completes within an acceptable time (e.g., 1 second)
    assert elapsed_time < 1, f"Performance test failed: Took {elapsed_time:.2f} seconds"

    # Optionally print results for debugging
    print(f"Performance test passed: Took {elapsed_time:.2f} seconds")
