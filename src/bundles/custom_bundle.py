import os
import sqlite3
import pandas as pd
from zipline.data.bundles import register
from zipline.utils.cli import maybe_show_progress

# Paths and configurations
DB_PATH = os.getenv("DB_PATH", "data/output/aligned_data.db")
CSV_PATH = os.getenv("CSV_PATH", "data/output/zipline_temp_data.csv")

def validate_columns(data):
    """Ensure all required columns are present."""
    required_columns = {"sid", "date", "open", "high", "low", "close", "volume"}
    missing_columns = required_columns - set(data.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    print("All required columns are present.")

def fetch_and_prepare_data():
    """Fetch data from the database and prepare it for ingestion."""
    print(f"DB_PATH: {DB_PATH}")
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
        # Fetch data
        data = pd.read_sql_query(query, connection)
        connection.close()
        print("Raw data fetched from the database:")
        print(data.head())

        # Validate columns
        validate_columns(data)

        # Drop rows with null dates
        data = data.dropna(subset=["date"])
        print("Data after dropping null dates:")
        print(data.head())

        # Filter placeholder rows
        data = data[
            (data["open"] != 0) &
            (data["high"] != 0) &
            (data["low"] != 0) &
            (data["close"] != 0)
        ]
        print("Data after filtering placeholder rows:")
        print(data.head())

        # Write cleaned data to CSV
        data.to_csv(CSV_PATH, index=False)
        print(f"Equity data successfully written to {CSV_PATH}")

        return data
    except Exception as e:
        print(f"Error during data fetch and preparation: {e}")
        raise

def custom_bundle(
    environ,
    asset_db_writer,
    minute_bar_writer,
    daily_bar_writer,
    adjustment_writer,
    calendar,
    start_session,
    end_session,
    cache,
    show_progress,
    output_dir,
):
    """Custom data bundle for ingestion."""
    # Fetch and prepare data
    data = fetch_and_prepare_data()

    # Map assets to integer IDs
    unique_sids = data["sid"].unique()
    sid_map = {sid: i for i, sid in enumerate(unique_sids)}
    print(f"Asset mapping: {sid_map}")

    # Group data by sid
    def data_generator():
        for sid, group in data.groupby("sid"):
            yield sid_map[sid], group

    try:
        daily_bar_writer.write(data_generator(), show_progress=show_progress)
    except Exception as e:
        print(f"Error during ingestion: {e}")
        raise

# Register the custom bundle
register("custom_csv", custom_bundle)

if __name__ == "__main__":
    from zipline.data.bundles.core import ingest

    try:
        print(f"ZIPLINE_ROOT: {os.getenv('ZIPLINE_ROOT', 'data/zipline_root')}")
        ingest("custom_csv")
    except Exception as e:
        print(f"Error: {e}")
