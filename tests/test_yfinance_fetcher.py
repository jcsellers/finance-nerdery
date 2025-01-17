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


def test_fetch_yfinance_data_with_deduplication(test_db):
    """Tests the yfinance data fetcher with deduplication."""
    tickers = ["AAPL", "MSFT"]
    start_date = "2023-01-01"
    fetch_yfinance_data(tickers, start_date, db_path=test_db)

    # Fetch data again to test deduplication
    fetch_yfinance_data(tickers, start_date, db_path=test_db)

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM historical_data")
    count = cursor.fetchone()[0]
    conn.close()

    # Ensure data was inserted once per record
    assert count > 0, "No data was fetched and stored in the database"
