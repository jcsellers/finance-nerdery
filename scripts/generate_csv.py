import sqlite3
import pandas as pd
import os

# Paths
DB_PATH = os.getenv("DB_PATH", "data/output/aligned_data.db")
CSV_PATH = os.getenv("CSV_PATH", "data/output/generated_data.csv")

def generate_csv_from_db():
    """
    Generate a CSV file for Zipline ingestion, filtering based on data conformity.
    """
    print(f"DB_PATH: {DB_PATH}")
    print(f"CSV_PATH: {CSV_PATH}")

    # Connect to the database
    connection = sqlite3.connect(DB_PATH)

    # Fetch all data
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
    data_df = pd.read_sql_query(query, connection)
    data_df["date"] = pd.to_datetime(data_df["date"], errors="coerce")
    data_df = data_df.dropna(subset=["date"])

    print(f"Initial data rows: {len(data_df)}")
    print(f"Initial data sample:\n{data_df.head()}")

    # Filter rows with invalid data
    valid_data = data_df[
        (data_df["open"] >= 0)
        & (data_df["high"] >= 0)
        & (data_df["low"] >= 0)
        & (data_df["close"] >= 0)
        & (data_df["volume"] >= 0)
    ]

    print(f"Filtered data rows: {len(valid_data)}")
    print(f"Filtered data sample:\n{valid_data.head()}")

    # Save the valid data to a CSV
    try:
        valid_data.to_csv(CSV_PATH, index=False)
        print(f"Valid data successfully written to {CSV_PATH}")
    except Exception as e:
        print(f"Error writing valid data: {e}")
        raise

    connection.close()
    return valid_data

if __name__ == "__main__":
    try:
        generate_csv_from_db()
    except Exception as e:
        print(f"Error during execution: {e}")
