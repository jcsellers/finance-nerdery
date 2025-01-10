import sqlite3
import pandas as pd
from src.bundles.custom_bundle import generate_csv_from_db


def test_generate_csv_from_db(tmp_path):
    """Test CSV generation from SQLite database."""
    # Define paths for the test database and temporary CSV file
    test_db_path = tmp_path / "test_aligned_data.db"
    temp_csv_path = tmp_path / "temp_data.csv"

    # Create a temporary SQLite database for testing
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

    # Insert test data into the `data` table
    cursor.execute(
        """
        INSERT INTO data (ticker, Date, Open, High, Low, Close, Volume)
        VALUES ('TEST', '2023-01-01', 100, 110, 90, 105, 1000);
        """
    )

    # Commit the changes and close the database connection
    connection.commit()
    connection.close()

    # Generate the CSV file using the function under test
    generate_csv_from_db(db_path=str(test_db_path), csv_path=str(temp_csv_path))

    # Validate that the CSV file was created successfully
    assert temp_csv_path.exists(), f"Expected CSV file {temp_csv_path} not found."

    # Validate the contents of the generated CSV file
    df = pd.read_csv(temp_csv_path)

    # Validate the number of rows in the CSV
    assert len(df) == 1, f"Expected 1 row in the CSV, but found {len(df)} rows."

    # Validate the values in the CSV file
    assert df.iloc[0]["sid"] == "TEST", f"Expected ticker 'TEST', but found {df.iloc[0]['sid']}."
    assert df.iloc[0]["close"] == 105, f"Expected close price 105, but found {df.iloc[0]['close']}."
