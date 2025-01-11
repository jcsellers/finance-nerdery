import sqlite3

import pandas as pd

from src.bundles.custom_bundle import fetch_and_prepare_data, validate_columns


def test_fetch_and_prepare_data(tmp_path):
    """Test data fetching and preparation for ingestion."""
    test_db_path = tmp_path / "test_aligned_data.db"
    temp_csv_path = tmp_path / "temp_data.csv"

    # Override global paths for the test
    global DB_PATH, CSV_PATH
    DB_PATH = str(test_db_path)
    CSV_PATH = str(temp_csv_path)

    # Create the SQLite test database
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
        VALUES ('TEST', '2023-01-01', 100, 110, 90, 105, 1000),
               ('TEST', '2023-01-02', 0, 0, 0, 0, 0); -- Placeholder row
        """
    )

    connection.commit()
    connection.close()

    # Run the fetch and prepare data function
    data = fetch_and_prepare_data()

    # Validate the fetched data
    assert len(data) == 1, "Placeholder row should be filtered out."
    assert data.iloc[0]["sid"] == "TEST"
    assert data.iloc[0]["close"] == 105

    # Validate the CSV output
    assert temp_csv_path.exists(), f"Expected CSV file {temp_csv_path} not found."
    df = pd.read_csv(temp_csv_path)
    assert len(df) == 1
    assert df.iloc[0]["sid"] == "TEST"
    assert df.iloc[0]["close"] == 105

    # Ensure columns match expectations
    validate_columns(data)
