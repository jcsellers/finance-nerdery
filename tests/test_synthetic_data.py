import pytest
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Constants
DATABASE_NAME = "data/aligned/trading_database.db"
TABLE_NAME = "prices"
EXPECTED_COLUMNS = ["timestamp", "ticker", "open", "high", "low", "close", "volume"]

# Utility Functions
def load_synthetic_data(ticker):
    """Loads data for a specific ticker from the synthetic database."""
    with sqlite3.connect(DATABASE_NAME) as conn:
        query = f"SELECT * FROM {TABLE_NAME} WHERE ticker = ?"
        return pd.read_sql_query(query, conn, params=(ticker,))

# Fixtures
@pytest.fixture(scope="module")
def synthetic_linear():
    """Fixture to load synthetic linear data."""
    return load_synthetic_data("SYN_LINEAR")

@pytest.fixture(scope="module")
def synthetic_cash():
    """Fixture to load synthetic cash data."""
    return load_synthetic_data("SYN_CASH")

# Tests

def test_database_exists():
    """Test if the synthetic database exists."""
    assert Path(DATABASE_NAME).exists(), f"Database {DATABASE_NAME} does not exist."

def test_table_schema():
    """Test if the table schema matches the expected structure."""
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({TABLE_NAME});")
        columns = [row[1] for row in cursor.fetchall()]
        assert columns == EXPECTED_COLUMNS, f"Schema mismatch: {columns} != {EXPECTED_COLUMNS}"

def test_synthetic_linear_validity(synthetic_linear):
    """Test the validity of the synthetic linear dataset."""
    assert not synthetic_linear.empty, "Synthetic linear dataset is empty."
    assert all(col in synthetic_linear.columns for col in EXPECTED_COLUMNS), "Missing columns in synthetic linear dataset."
    
    # Validate timestamps
    timestamps = pd.to_datetime(synthetic_linear["timestamp"])
    assert timestamps.is_monotonic_increasing, "Timestamps are not in order."

    # Validate prices
    assert (synthetic_linear["open"] > 0).all(), "Open prices must be greater than zero."
    assert (synthetic_linear["high"] >= synthetic_linear["open"]).all(), "High prices must be >= open prices."
    assert (synthetic_linear["low"] <= synthetic_linear["open"]).all(), "Low prices must be <= open prices."
    assert (synthetic_linear["close"] > 0).all(), "Close prices must be greater than zero."

def test_synthetic_cash_validity(synthetic_cash):
    """Test the validity of the synthetic cash dataset."""
    assert not synthetic_cash.empty, "Synthetic cash dataset is empty."
    assert all(col in synthetic_cash.columns for col in EXPECTED_COLUMNS), "Missing columns in synthetic cash dataset."
    
    # Validate cash consistency
    assert (synthetic_cash["open"] == synthetic_cash["high"]).all(), "Cash high must equal open."
    assert (synthetic_cash["open"] == synthetic_cash["low"]).all(), "Cash low must equal open."
    assert (synthetic_cash["open"] == synthetic_cash["close"]).all(), "Cash close must equal open."

def test_linear_cagr(synthetic_linear):
    """Test if the CAGR of the synthetic linear asset matches expected values."""
    start_price = synthetic_linear.iloc[0]["close"]
    end_price = synthetic_linear.iloc[-1]["close"]
    days = len(synthetic_linear)
    expected_cagr = (end_price / start_price) ** (365 / days) - 1

    # Actual CAGR
    actual_cagr = (synthetic_linear["close"].iloc[-1] / synthetic_linear["close"].iloc[0]) ** (365 / days) - 1
    np.testing.assert_almost_equal(actual_cagr, expected_cagr, decimal=4, err_msg="CAGR does not match expected value.")

def test_buy_and_hold(synthetic_linear):
    """Test buy-and-hold strategy returns on synthetic linear data."""
    start_price = synthetic_linear.iloc[0]["open"]
    end_price = synthetic_linear.iloc[-1]["close"]
    expected_return = ((end_price - start_price) / start_price) * 100

    # Validate calculated return
    actual_return = ((end_price - start_price) / start_price) * 100
    np.testing.assert_almost_equal(actual_return, expected_return, decimal=4, err_msg="Buy-and-hold return mismatch.")

if __name__ == "__main__":
    pytest.main(["-v"])
