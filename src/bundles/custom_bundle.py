import os
import sqlite3

import pandas as pd
from zipline.data.bundles import register

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

    # Validate columns
    required_columns = {"sid", "date", "open", "high", "low", "close", "volume"}
    missing_columns = required_columns - set(data.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    print("All required columns are present.")

    # Drop rows with invalid dates or placeholder data
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = data.dropna(subset=["date"])
    data = data[data["date"] >= pd.Timestamp("1990-01-02")]  # Filter dates before 1990
    data = data[data["date"] <= pd.Timestamp("2025-01-03")]  # Filter dates after 2025

    # Check for invalid numeric values
    numeric_columns = ["open", "high", "low", "close", "volume"]
    for col in numeric_columns:
        invalid_values = data[data[col] < 0]
        if not invalid_values.empty:
            print(f"Warning: Found invalid values in column '{col}':")
            print(invalid_values)

        data = data[data[col] >= 0]  # Filter out invalid numeric values

    print("Data after cleaning:")
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
    print("Starting data ingestion process.")

    # Fetch and prepare data
    data = fetch_and_prepare_data()

    # Align data to the trading calendar
    trading_days = calendar.sessions_in_range(start_session, end_session)

    # Map assets to integer IDs
    unique_sids = data["sid"].unique()
    sid_map = {sid: i for i, sid in enumerate(unique_sids)}
    print(f"Asset mapping: {sid_map}")

    # Group data by sid and align dates to the trading calendar
    def data_generator():
        for sid, group in data.groupby("sid"):
            group["date"] = pd.to_datetime(group["date"])
            group = group.set_index("date").reindex(trading_days).reset_index()
            group.fillna(method="ffill", inplace=True)
            group.fillna(method="bfill", inplace=True)

            # Drop rows with still-missing data
            group.dropna(inplace=True)

            yield sid_map[sid], group

    # Write data to daily bar writer
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
        print(f"ZIPLINE_ROOT: {os.getenv('ZIPLINE_ROOT', 'data/zipline_root')}")
        ingest("custom_csv")
    except Exception as e:
        print(f"Error during ingestion: {e}")
