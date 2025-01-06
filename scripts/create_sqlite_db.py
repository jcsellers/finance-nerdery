import sqlite3
import pandas as pd
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the path to the SQLite database
db_path = '../data/aligned_data.db'

# List of CSV files and corresponding table names
datasets = {
    "UPRO": "../data/aligned/UPRO_cleaned.csv",
    "SSO": "../data/aligned/SSO_cleaned.csv",
    "SPY": "../data/aligned/SPY_cleaned.csv",
    "TLT": "../data/aligned/TLT_cleaned.csv",
    "GLD": "../data/aligned/GLD_cleaned.csv",
    "^SPX": "../data/aligned/^SPX_cleaned.csv",
    "^VIX": "../data/aligned/^VIX_cleaned.csv",
    "DGS10": "../data/aligned/DGS10_cleaned.csv",
    "BAMLH0A0HYM2": "../data/aligned/BAMLH0A0HYM2_cleaned.csv",
}

def create_and_populate_database(db_path, datasets):
    """
    Create a SQLite database and populate it with data from CSV files.
    :param db_path: Path to the SQLite database.
    :param datasets: Dictionary with table names as keys and CSV file paths as values.
    """
    # Connect to the SQLite database
    try:
        conn = sqlite3.connect(db_path)
        logging.info(f"Connected to SQLite database at {db_path}")
    except sqlite3.Error as e:
        logging.error(f"Error connecting to SQLite database: {e}")
        return

    # Create tables and import data
    for table_name, csv_path in datasets.items():
        try:
            # Check if the CSV file exists
            if not os.path.exists(csv_path):
                logging.warning(f"File {csv_path} does not exist. Skipping.")
                continue

            # Load data from the CSV file
            df = pd.read_csv(csv_path)

            # Ensure 'Date' column exists
            if 'Date' not in df.columns:
                logging.warning(f"File {csv_path} does not contain a 'Date' column. Skipping.")
                continue

            # Set 'Date' as the index and convert to string for SQLite compatibility
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            df.set_index('Date', inplace=True)

            # Write the data to the SQLite table
            df.to_sql(table_name, conn, if_exists='replace', index=True, index_label='Date')
            logging.info(f"Table '{table_name}' created and populated from {csv_path}")
        except Exception as e:
            logging.error(f"Error processing {csv_path}: {e}")

    # Close the database connection
    conn.close()
    logging.info("Database connection closed.")

# Execute the script
if __name__ == "__main__":
    create_and_populate_database(db_path, datasets)
