import os
import sqlite3


def create_test_db(db_path):
    """Create a deterministic test database."""
    # Connect to SQLite and create the deterministic database
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    # Create the `data` table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS data (
        ticker TEXT,
        Date TEXT,
        Open REAL,
        High REAL,
        Low REAL,
        Close REAL,
        Volume INTEGER
    );
    """)

    # Insert deterministic data
    deterministic_data = [
        ("TEST", "2023-01-01", 100, 110, 90, 105, 1000),
        ("TEST", "2023-01-02", 105, 115, 95, 110, 1200),
        ("TEST", "2023-01-03", 110, 120, 100, 115, 1300),
        ("TEST", "2023-01-04", 115, 125, 105, 120, 1400),
        ("TEST", "2023-01-05", 120, 130, 110, 125, 1500),
    ]

    cursor.executemany("""
    INSERT INTO data (ticker, Date, Open, High, Low, Close, Volume)
    VALUES (?, ?, ?, ?, ?, ?, ?);
    """, deterministic_data)

    # Commit and close the database
    connection.commit()
    connection.close()
    print(f"Test database created at: {db_path}")


if __name__ == "__main__":
    test_db_path = "../data/output/deterministic_test_data.db"
    os.makedirs(os.path.dirname(test_db_path), exist_ok=True)
    create_test_db(test_db_path)
