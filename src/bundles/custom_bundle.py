import os
import sqlite3
import pandas as pd
from zipline.data.bundles import register, ingest
from zipline.utils.cli import maybe_show_progress


# Paths and configurations
db_path = os.getenv("DB_PATH", "../../data/output/aligned_data.db")
csv_temp_path = os.getenv("TEMP_CSV_PATH", "../../data/output/zipline_temp_data.csv")


def generate_csv_from_db(db_path=None, csv_path=None):
    """Generate a CSV file from the SQLite database."""
    db_path = db_path or os.getenv("DB_PATH", "../../data/output/aligned_data.db")
    csv_path = csv_path or os.getenv("TEMP_CSV_PATH", "../../data/output/zipline_temp_data.csv")

    # Ensure the output directory exists
    csv_dir = os.path.dirname(csv_path)
    os.makedirs(csv_dir, exist_ok=True)

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")

    print(f"DB_PATH: {db_path}")
    print(f"TEMP_CSV_PATH: {csv_path}")

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
        print(f"Data fetched from DB:\n{data_df.head()}")
        if data_df["date"].isnull().any():
            print("WARNING: Null dates detected in the database.")
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise

    # Drop null dates early
    data_df["date"] = pd.to_datetime(data_df["date"], errors="coerce")
    data_df = data_df.dropna(subset=["date"])  # Drop invalid dates

    print(f"Data after dropping null dates:\n{data_df.head()}")

    try:
        data_df.to_csv(csv_path, index=False)
        print(f"CSV written to {csv_path}")
    except Exception as e:
        print(f"Error writing CSV: {e}")
        raise

    connection.close()


def custom_bundle(environ, asset_db_writer, minute_bar_writer, daily_bar_writer,
                  adjustment_writer, calendar, start_session, end_session, cache,
                  show_progress, output_dir):
    """Custom data bundle to ingest CSV data."""
    generate_csv_from_db()

    if not os.path.exists(csv_temp_path):
        raise FileNotFoundError(f"CSV file not found: {csv_temp_path}")

    print(f"CSV file ready for ingestion: {csv_temp_path}")

    data = pd.read_csv(csv_temp_path, parse_dates=["date"])
    print(f"Initial data rows: {len(data)}")

    # Remove invalid placeholder rows (e.g., 1970-01-01)
    invalid_placeholder_date = pd.Timestamp("1970-01-01")
    data = data[data["date"] != invalid_placeholder_date]
    print(f"Data after filtering placeholder dates:\n{data.head()}")

    # Fetch the database's date range
    db_connection = sqlite3.connect(db_path)
    db_date_range = pd.read_sql_query(
        "SELECT MIN(Date) AS start_date, MAX(Date) AS end_date FROM data;", db_connection
    )
    db_connection.close()
    db_start_date = pd.to_datetime(db_date_range["start_date"].iloc[0])
    db_end_date = pd.to_datetime(db_date_range["end_date"].iloc[0])
    print(f"Database date range: {db_start_date} to {db_end_date}")

    # Ensure dates are within range
    data = data[(data["date"] >= db_start_date) & (data["date"] <= db_end_date)]
    data = data[(data["date"] >= start_session) & (data["date"] <= end_session)]

    valid_sessions = calendar.sessions_in_range(start_session, end_session)
    extra_dates = data[~data["date"].isin(valid_sessions)]
    if not extra_dates.empty:
        print(f"Extra sessions:\n{extra_dates}")

    data = data[data["date"].isin(valid_sessions)]
    print(f"Validated data rows: {len(data)}")

    # Ensure column names and data types are compatible
    data = data.rename(columns={"date": "date", "sid": "sid"})
    data["sid"] = data["sid"].astype("category").cat.codes

    # Debugging: Loop detection
    loop_count = 0
    def data_generator():
        nonlocal loop_count
        for sid, df in data.groupby("sid"):
            loop_count += 1
            if loop_count > len(data):
                raise RuntimeError("Infinite loop detected in data generator.")
            yield sid, df

    # Pass data to Zipline's writer
    daily_bar_writer.write(data_generator(), show_progress=show_progress)


# Register the custom bundle
register("custom_csv", custom_bundle)

if __name__ == "__main__":
    os.environ["ZIPLINE_ROOT"] = os.path.abspath("data/zipline_root")
    print(f"ZIPLINE_ROOT: {os.environ['ZIPLINE_ROOT']}")

    try:
        ingest("custom_csv")
        print("Bundle ingestion completed successfully.")
    except Exception as e:
        print(f"Error during ingestion: {e}")
        raise
