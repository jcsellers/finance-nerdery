import sqlite3
import pandas as pd
from src.bundles.custom_bundle import fetch_and_prepare_data


def test_fetch_and_prepare_data(tmp_path):
    """Test data fetching and preparation from SQLite database."""
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
    cursor.executemany(
        """
        INSERT INTO data (ticker, Date, Open, High, Low, Close, Volume)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        [
            ("TEST", "2023-01-01", 100, 110, 90, 105, 1000),
            ("FRED", "2023-01-02", 3.0, 3.0, 3.0, 3.0, 0),
        ],
    )

    connection.commit()
    connection.close()

    # Replace environment variables for testing
    os.environ["DB_PATH"] = str(test_db_path)
    os.environ["CSV_PATH"] = str(temp_csv_path)

    # Fetch and prepare data
    data = fetch_and_prepare_data()

    # Validate the CSV
    assert temp_csv_path.exists(), f"Expected CSV file {temp_csv_path} not found."
    df = pd.read_csv(temp_csv_path)
    assert len(df) == 2
    assert df.iloc[0]["sid"] == "TEST"
    assert df.iloc[1]["sid"] == "FRED"
    assert df.iloc[0]["close"] == 105
    assert df.iloc[1]["close"] == 3.0
