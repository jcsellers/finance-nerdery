import sqlite3
import pandas as pd
from src.bundles.custom_bundle import generate_csv_from_db


def test_generate_csv_from_db(tmp_path):
    """Test CSV generation from SQLite database."""
    # Create a temporary SQLite database for testing
    test_db_path = tmp_path / "test_aligned_data.db"
    connection = sqlite3.connect(test_db_path)
    cursor = connection.cursor()

    # Create the data table
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

    # Insert test data into the table
    cursor.execute(
        """
    INSERT INTO data (ticker, Date, Open, High, Low, Close, Volume)
    VALUES ('TEST', '2023-01-01', 100, 110, 90, 105, 1000);
    """
    )

    # Commit and close the database
    connection.commit()
    connection.close()

    # Define the path for the temporary CSV file
    temp_csv_path = tmp_path / "temp_data.csv"

    # Generate the CSV using the function under test
    generate_csv_from_db(db_path=str(test_db_path), csv_path=str(temp_csv_path))

    # Validate that the CSV file exists
    assert temp_csv_path.exists(), f"Expected CSV file {temp_csv_path} not found."

    # Validate the contents of the CSV file
    df = pd.read_csv(temp_csv_path)
    assert len(df) == 1  # Only one row should be present
    assert df.iloc[0]["sid"] == "TEST"  # Check the ticker value
    assert df.iloc[0]["close"] == 105  # Check the closing price
