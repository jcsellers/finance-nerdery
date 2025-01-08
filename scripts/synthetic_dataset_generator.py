import sqlite3
import os
from datetime import datetime
from database_utils import save_to_database
from data_generation import generate_linear_trend, generate_cash_asset

# Database Path
DATABASE_PATH = "../data/aligned/trading_database.db"

def initialize_database(database_path):
    """
    Initialize the SQLite database with the required schema and UNIQUE constraint.
    """
    os.makedirs(os.path.dirname(database_path), exist_ok=True)
    with sqlite3.connect(database_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS prices (
                timestamp TEXT,
                ticker TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                UNIQUE(ticker, timestamp)
            );
            """
        )

def main():
    """
    Main function to generate synthetic datasets and integrate them into the database.
    """
    # Initialize the database
    initialize_database(DATABASE_PATH)

    # Generate synthetic datasets
    print("Generating synthetic datasets...")
    start_date = datetime(1990, 1, 2)
    days = (datetime.now() - start_date).days

    synthetic_linear = generate_linear_trend(100, start_date, days, ticker="SYN_LINEAR")
    synthetic_cash = generate_cash_asset(150000, start_date, days, ticker="SYN_CASH")

    # Save synthetic data to database
    print(f"Saving synthetic datasets: {len(synthetic_linear)} rows for SYN_LINEAR, {len(synthetic_cash)} rows for SYN_CASH...")
    save_to_database(synthetic_linear, DATABASE_PATH)
    save_to_database(synthetic_cash, DATABASE_PATH)

    print(f"Synthetic datasets integrated into the database at {DATABASE_PATH}.")

if __name__ == "__main
