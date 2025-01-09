import sqlite3
import os
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def create_and_populate_unified_table(db_path, datasets):
    """
    Create a unified SQLite database table and populate it with data from CSV files.

    Parameters:
        db_path (str): Path to the SQLite database.
        datasets (dict): Dictionary with tickers as keys and CSV file paths as values.
    """
    conn = None  # Initialize connection
    try:
        # Ensure the database directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        logging.info(f"Connected to SQLite database at {db_path}")

        # Create the unified `data` table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data (
                ticker TEXT NOT NULL,
                Date TEXT NOT NULL,
                Open REAL,
                Close REAL,
                High REAL,
                Low REAL,
                Volume INTEGER,
                PRIMARY KEY (ticker, Date)
            )
        """)
        logging.info("Table 'data' created.")

        # Populate the `data` table
        for ticker, file_path in datasets.items():
            if not os.path.exists(file_path):
                logging.warning(f"File {file_path} does not exist. Skipping.")
                continue

            try:
                df = pd.read_csv(file_path)

                # Ensure required columns are present
                for col in ["High", "Low", "Volume"]:
                    if col not in df.columns:
                        logging.warning(f"Column '{col}' missing in {file_path}. Filling with default values.")
                        df[col] = 0  # Default value

                # Remove duplicates
                df.drop_duplicates(subset=["ticker", "Date"], inplace=True)

                # Add ticker column and process dates
                df["ticker"] = ticker
                df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

                # Insert rows into the database, handling duplicates with REPLACE INTO
                for _, row in df.iterrows():
                    try:
                        cursor.execute("""
                            INSERT OR REPLACE INTO data (ticker, Date, Open, Close, High, Low, Volume)
                            VALUES (?, ?, ?, ?, ?, ?, ?);
                        """, (row["ticker"], row["Date"], row["Open"], row["Close"], row["High"], row["Low"], row["Volume"]))
                    except sqlite3.Error as e:
                        logging.error(f"Error inserting row for {ticker}: {e}")

                logging.info(f"Data for '{ticker}' added to 'data' table.")
            except Exception as e:
                logging.error(f"Error processing {file_path}: {e}")

        conn.commit()

        # Confirm database row count
        cursor.execute("SELECT COUNT(*) FROM data;")
        total_rows = cursor.fetchone()[0]
        logging.info(f"Total rows in database: {total_rows}")

    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")

    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

