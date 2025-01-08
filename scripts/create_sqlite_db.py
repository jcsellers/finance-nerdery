import sqlite3
import pandas as pd
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define the path to the SQLite database
DB_PATH = "../data/aligned_data.db"

# List of CSV files and their tickers
DATASETS = {
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

# Default columns and types
REQUIRED_COLUMNS = {
    "Date": "TEXT",
    "Open": "REAL",
    "Close": "REAL",
    "High": "REAL",
    "Low": "REAL",
    "Volume": "INTEGER",
}


def create_and_populate_unified_table(db_path, datasets):
    """
    Create a unified SQLite database table and populate it with data from CSV files.

    Parameters:
        db_path (str): Path to the SQLite database.
        datasets (dict): Dictionary with tickers as keys and CSV file paths as values.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        logging.info(f"Connected to SQLite database at {db_path}")

        # Create the unified `data` table
        columns_definition = ", ".join([f"{col} {col_type}" for col, col_type in REQUIRED_COLUMNS.items()])
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS data (
                ticker TEXT NOT NULL,
                {columns_definition},
                PRIMARY KEY (ticker, Date)
            )
            """
        )
        logging.info("Table 'data' created.")

        # Populate the `data` table
        for ticker, csv_path in datasets.items():
            try:
                if not os.path.exists(csv_path):
                    logging.warning(f"File {csv_path} does not exist. Skipping.")
                    continue

                df = pd.read_csv(csv_path)

                # Ensure required columns are present
                for col in REQUIRED_COLUMNS:
                    if col not in df.columns:
                        logging.warning(f"Column '{col}' missing in {csv_path}. Filling with default values.")
                        default_value = 0 if REQUIRED_COLUMNS[col] in ["REAL", "INTEGER"] else None
                        df[col] = default_value

                # Add ticker column and process dates
                df["ticker"] = ticker
                df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

                # Write to the database
                df.to_sql("data", conn, if_exists="append", index=False)
                logging.info(f"Data for '{ticker}' added to 'data' table.")
            except Exception as e:
                logging.error(f"Error processing {csv_path}: {e}")

        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
    finally:
        conn.close()
        logging.info("Database connection closed.")


# Execute the script
if __name__ == "__main__":
    create_and_populate_unified_table(DB_PATH, DATASETS)
