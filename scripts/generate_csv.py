import os
import sqlite3
import pandas as pd


def generate_csv(db_path=None, csv_path=None):
    """
    Generate a CSV file from the SQLite database.

    This script extracts data from the `data` table, validates it,
    and writes the cleaned data to a CSV file.
    """
    # Default paths
    db_path = db_path or os.getenv("DB_PATH", "data/output/aligned_data.db")
    csv_path = csv_path or os.getenv("CSV_PATH", "data/output/generated_data.csv")

    # Ensure the output directory exists
    csv_dir = os.path.dirname(csv_path)
    os.makedirs(csv_dir, exist_ok=True)

    # Check database existence
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")

    print(f"DB_PATH: {db_path}")
    print(f"CSV_PATH: {csv_path}")

    # Connect to the database and fetch data
    connection = sqlite3.connect(db_path)
    query = """
    SELECT
        ticker AS sid,
        Date AS date,
        Open AS Open,
        High AS High,
        Low AS Low,
        Close AS Close,
        Volume AS Volume
    FROM data;
    """
    try:
        data_df = pd.read_sql_query(query, connection)
        data_df["date"] = pd.to_datetime(data_df["date"])
        print(f"Data fetched from database:\n{data_df.head()}")
        print(f"Columns in DataFrame: {data_df.columns.tolist()}")
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise

    # Validate data: remove rows with zero prices
    required_columns = ["High", "Low", "Open", "Close"]
    for column in required_columns:
        if column not in data_df.columns:
            raise KeyError(f"Missing column in DataFrame: {column}")

    data_df = data_df[
        (data_df["High"] != 0) &
        (data_df["Low"] != 0) &
        (data_df["Open"] != 0) &
        (data_df["Close"] != 0)
    ]
    print(f"Data after filtering zero prices:\n{data_df.head()}")

    # Write the cleaned data to CSV
    try:
        data_df.to_csv(csv_path, index=False)
        print(f"CSV successfully written to {csv_path}")
    except Exception as e:
        print(f"Error writing CSV: {e}")
        raise

    # Close the database connection
    connection.close()


if __name__ == "__main__":
    generate_csv()
