import os
import sqlite3
import pandas as pd
from zipline.data.bundles import register
from src.utils.data_validation import validate_columns, validate_asset_metadata
# Paths and configurations
DB_PATH = os.getenv("DB_PATH", "data/output/aligned_data.db")
CSV_PATH = os.getenv("CSV_PATH", "data/output/zipline_temp_data.csv")


def fetch_and_prepare_data():
    """Fetch data from the database and prepare for ingestion."""
    print(f"DB_PATH: {DB_PATH}")
    connection = sqlite3.connect(DB_PATH)
    query = """
    SELECT ticker AS sid, Date AS date, Open AS open, High AS high, Low AS low, Close AS close, Volume AS volume
    FROM data;
    """
    data = pd.read_sql_query(query, connection)
    connection.close()

    print("Raw data fetched from the database:")
    print(data.head())

    validate_columns(data)
    validate_asset_metadata(data)

    # Drop rows with null dates
    data = data.dropna(subset=["date"])
    print("Data after dropping null dates:")
    print(data.head())

    # Save cleaned data to a CSV file
    data.to_csv(CSV_PATH, index=False)
    print(f"Equity data successfully written to {CSV_PATH}")

    return data


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
    """Custom bundle for ingesting data."""
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
        print(f"Ingested {len(unique_sids)} assets successfully.")
    except Exception as e:
        raise RuntimeError(f"Error during ingestion: {e}")

# Register the custom bundle
register("custom_csv", custom_bundle)

if __name__ == "__main__":
    from zipline.data.bundles.core import ingest

    try:
        ingest("custom_csv")
    except Exception as e:
        print(f"Error during ingestion: {e}")
