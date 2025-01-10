import os
import sqlite3
import pandas as pd


def generate_csv(db_path=None, csv_path=None):
    """Generate a CSV file from the SQLite database."""
    db_path = db_path or os.getenv("DB_PATH", "data/output/aligned_data.db")
    csv_path = csv_path or os.getenv("CSV_PATH", "data/output/generated_data.csv")

    # Ensure the output directory exists
    csv_dir = os.path.dirname(csv_path)
    os.makedirs(csv_dir, exist_ok=True)

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")

    print(f"DB_PATH: {db_path}")
    print(f"CSV_PATH: {csv_path}")

    connection = sqlite3.connect(db_path)
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
        data_df = pd.read_sql_query(query, connection)
        data_df["date"] = pd.to_datetime(data_df["date"])
        print(f"Data fetched: {data_df.head()}")
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise

    try:
        data_df.to_csv(csv_path, index=False)
        print(f"CSV written to {csv_path}")
    except Exception as e:
        print(f"Error writing CSV: {e}")
        raise

    connection.close()


if __name__ == "__main__":
    generate_csv()
