import sqlite3

# Path to the database
db_path = "../data/finance_data.db"

# Connect to the database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create or update the `daily_prices` table with the correct schema
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS daily_prices (
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        symbol TEXT
    );
"""
)

conn.commit()
conn.close()

print("Table schema updated successfully.")
