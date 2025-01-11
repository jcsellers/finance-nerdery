import os
import sqlite3

import pandas as pd

# Paths
DB_PATH = os.getenv("DB_PATH", "data/output/aligned_data.db")
CSV_PATH = os.getenv("CSV_PATH", "data/output/generated_data.csv")


def validate_and_clean_data(data):
    """Ensure data conforms to required structure."""
    required_columns = {"sid", "date", "open", "high", "low", "close", "volume"}
    missing_columns = required_columns - set(data.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = data.dropna(subset=["date"])
    data = data[
        (data["open"] >= 0)
        & (data["high"] >= 0)
        & (data["low"] >= 0)
        & (data["close"] >= 0)
        & (data["volume"] >= 0)
    ]
    return data


def generate_csv_from_db():
    """Generate a CSV file for Zipline ingestion."""
    print(f"DB_PATH: {DB_PATH}")
    print(f"CSV_PATH: {CSV_PATH}")

    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database file not found at: {DB_PATH}")

    connection = sqlite3.connect(DB_PATH)
    query = """
    SELECT
        ticker AS sid,
        Date AS date,
        Open AS open,
        High AS high,
        Low AS low,
        Close AS close,
        Volume AS volume
    FROM data;
    """
    try:
        data = pd.read_sql_query(query, connection)
        connection.close()

        print("Raw data fetched from the database:")
        print(data.head())

        # Validate and clean data
        valid_data = validate_and_clean_data(data)
        print("Validated and cleaned data:")
        print(valid_data.head())

        # Save the valid data to a CSV
        valid_data.to_csv(CSV_PATH, index=False)
        print(f"Data successfully written to {CSV_PATH}")
    except Exception as e:
        print(f"Error during data generation: {e}")
        raise


if __name__ == "__main__":
    try:
        generate_csv_from_db()
    except Exception as e:
        print(f"Error during execution: {e}")
