import os
import sqlite3

import pandas as pd
import pytest

from src.yfinance_fetcher import fetch_yfinance_data


@pytest.fixture
def test_db(tmp_path):
    """Creates a temporary SQLite database for testing."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS historical_data (
            ticker TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adjusted_close REAL,
            volume INTEGER,
            source TEXT DEFAULT 'yfinance',
            is_filled BOOLEAN DEFAULT 0,
            PRIMARY KEY (ticker, timestamp)
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def intermediate_dir(tmp_path):
    """Creates a temporary directory for storing CSV files."""
    dir_path = tmp_path / "csv_files"
    os.makedirs(dir_path, exist_ok=True)
    return dir_path


def validate_data_integrity(db_path, expected_tickers):
    """Validates the data integrity in the SQLite database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for ticker in expected_tickers:
        cursor.execute(
            "SELECT COUNT(*), COUNT(DISTINCT timestamp) FROM historical_data WHERE ticker = ?",
            (ticker,),
        )
        row_count, unique_timestamps = cursor.fetchone()

        assert row_count > 0, f"No data was fetched for ticker {ticker}"
        assert (
            row_count == unique_timestamps
        ), f"Duplicate timestamps found for ticker {ticker}"

    conn.close()


def test_fetch_yfinance_data_with_deduplication(test_db, intermediate_dir):
    """Tests the yfinance data fetcher with deduplication and CSV creation."""
    tickers = ["AAPL", "MSFT"]
    start_date = "2023-01-01"

    # Initial fetch
    fetch_yfinance_data(
        tickers, start_date, db_path=test_db, intermediate_dir=intermediate_dir
    )

    # Validate database records after first fetch
    validate_data_integrity(test_db, tickers)

    # Validate CSV files were created
    for ticker in tickers:
        csv_path = os.path.join(intermediate_dir, f"{ticker}_data.csv")
        assert os.path.exists(csv_path), f"CSV file for {ticker} not found!"
        csv_data = pd.read_csv(csv_path)
        assert not csv_data.empty, f"CSV file for {ticker} is empty!"

    # Fetch data again to test deduplication
    fetch_yfinance_data(
        tickers, start_date, db_path=test_db, intermediate_dir=intermediate_dir
    )

    # Ensure data integrity and no duplicates in the database
    validate_data_integrity(test_db, tickers)

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM historical_data")
    count_after_second_fetch = cursor.fetchone()[0]
    conn.close()

    # Ensure data was not duplicated
    assert count_after_second_fetch == len(csv_data) * len(
        tickers
    ), "Data duplication detected"


def test_fetch_yfinance_data_with_invalid_ticker(test_db, intermediate_dir):
    """Tests fetching data for an invalid ticker symbol."""
    tickers = ["INVALID"]
    start_date = "2023-01-01"

    fetch_yfinance_data(
        tickers, start_date, db_path=test_db, intermediate_dir=intermediate_dir
    )

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM historical_data")
    count = cursor.fetchone()[0]
    conn.close()

    # No data should be inserted for invalid tickers
    assert count == 0, "Data should not be fetched for invalid ticker"

    # Ensure no CSVs are created
    csv_path = os.path.join(intermediate_dir, "INVALID_data.csv")
    assert not os.path.exists(csv_path), "CSV file created for invalid ticker"


def test_fetch_yfinance_data_with_partial_valid_tickers(test_db, intermediate_dir):
    """Tests fetching data where some tickers are valid and others are invalid."""
    tickers = ["AAPL", "INVALID"]
    start_date = "2023-01-01"

    fetch_yfinance_data(
        tickers, start_date, db_path=test_db, intermediate_dir=intermediate_dir
    )

    # Validate data integrity for valid tickers only
    validate_data_integrity(test_db, ["AAPL"])

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT ticker FROM historical_data")
    fetched_tickers = [row[0] for row in cursor.fetchall()]
    conn.close()

    # Ensure only valid tickers are present in the database
    assert (
        "INVALID" not in fetched_tickers
    ), "Invalid ticker data should not be in the database"

    # Validate CSV files for valid tickers only
    csv_path_valid = os.path.join(intermediate_dir, "AAPL_data.csv")
    assert os.path.exists(csv_path_valid), "CSV file for AAPL not found!"
