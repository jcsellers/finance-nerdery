import sqlite3
import pandas as pd
import os
from src.bundles.custom_bundle import fetch_and_prepare_data

def test_fetch_and_prepare_data(tmp_path):
    """Test the fetch and prepare data function."""
    test_db_path = tmp_path / "test_aligned_data.db"
    temp_csv_path = tmp_path / "temp_data.csv"

    connection = sqlite3.connect(test_db_path)
    cursor = connection.cursor()

    # Create the `data` table schema
    cursor.execute(
        """
        CREATE TABLE data (
            ticker TEXT,
            Date TEXT,
            Open REAL,
            High REAL,
            Low REAL,
            Close REAL,
            Volume INTEGER
        );
        """
    )

    # Insert test data
    cursor.execute(
        """
        INSERT INTO data (ticker, Date, Open, High, Low, Close, Volume)
        VALUES ('TEST', '2023-01-01', 100, 110, 90, 105, 1000);
        """
    )

    connection.commit()
    connection.close()

    # Mock environment variables
    os.environ["DB_PATH"] = str(test_db_path)
    os.environ["CSV_PATH"] = str(temp_csv_path)

    # Fetch and prepare data
    data = fetch_and_prepare_data()

    # Validate the data
    assert len(data) == 1
    assert data.iloc[0]["sid"] == "TEST"
    assert data.iloc[0]["close"] == 105
