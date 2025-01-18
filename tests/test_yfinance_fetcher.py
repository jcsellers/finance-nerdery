import sqlite3

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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adjusted_close REAL,
            volume INTEGER,
            source TEXT DEFAULT 'yfinance',
            is_filled BOOLEAN DEFAULT 0
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


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


def test_fetch_yfinance_data_with_deduplication(test_db):
    """Tests the yfinance data fetcher with deduplication."""
    tickers = ["AAPL", "MSFT"]
    start_date = "2023-01-01"

    # Initial fetch
    fetch_yfinance_data(tickers, start_date, db_path=test_db)

    # Validate initial fetch data integrity
    validate_data_integrity(test_db, tickers)

    # Check database records after first fetch
    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM historical_data")
    count_after_first_fetch = cursor.fetchone()[0]
    conn.close()
    assert count_after_first_fetch > 0, "No data was fetched in the first fetch"

    # Fetch data again to test deduplication
    fetch_yfinance_data(tickers, start_date, db_path=test_db)

    # Validate data integrity post deduplication
    validate_data_integrity(test_db, tickers)

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM historical_data")
    count_after_second_fetch = cursor.fetchone()[0]
    conn.close()

    # Ensure data was not duplicated
    assert (
        count_after_second_fetch == count_after_first_fetch
    ), "Data duplication detected"


def test_fetch_yfinance_data_with_invalid_ticker(test_db):
    """Tests fetching data for an invalid ticker symbol."""
    tickers = ["INVALID"]
    start_date = "2023-01-01"

    fetch_yfinance_data(tickers, start_date, db_path=test_db)

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM historical_data")
    count = cursor.fetchone()[0]
    conn.close()

    assert count == 0, "Data should not be fetched for invalid ticker"


def test_fetch_yfinance_data_with_partial_valid_tickers(test_db):
    """Tests fetching data where some tickers are valid and others are invalid."""
    tickers = ["AAPL", "INVALID"]
    start_date = "2023-01-01"

    fetch_yfinance_data(tickers, start_date, db_path=test_db)

    # Validate data integrity for valid tickers only
    validate_data_integrity(test_db, ["AAPL"])

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT ticker FROM historical_data")
    fetched_tickers = [row[0] for row in cursor.fetchall()]
    conn.close()

    assert (
        "INVALID" not in fetched_tickers
    ), "Invalid ticker data should not be in the database"
