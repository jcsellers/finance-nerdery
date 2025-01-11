import os
import sqlite3
import pandas as pd
from zipline.data.bundles import ingest, register
from zipline.utils.cli import maybe_show_progress

# Paths
DB_PATH = os.getenv("DB_PATH", "data/output/aligned_data.db")
CSV_PATH = os.getenv("CSV_PATH", "data/output/zipline_temp_data.csv")


def generate_asset_mapping(data):
    """
    Map all asset identifiers (sids) to integer IDs.
    """
    sid_to_asset_id = {sid: idx for idx, sid in enumerate(data["sid"].unique())}
    data["sid"] = data["sid"].map(sid_to_asset_id).astype("uint32")
    print(f"Asset mapping: {sid_to_asset_id}")
    return data


def validate_data(data):
    """
    Ensure data conforms to Zipline's requirements.
    """
    print(f"Initial data rows: {len(data)}")
    print(f"Initial data:\n{data.head()}")

    # Drop invalid rows (e.g., null or placeholder values)
    data = data.dropna(subset=["date"])
    data = data[
        (data["open"] != 0) &
        (data["high"] != 0) &
        (data["low"] != 0) &
        (data["close"] != 0)
    ]
    print(f"Validated data rows: {len(data)}")
    return data


def generate_csv_from_db():
    """
    Fetch data from the database and generate a CSV file.
    """
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
    data = pd.read_sql_query(query, connection)
    connection.close()

    # Convert 'date' to datetime and validate
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = validate_data(data)

    # Write validated data to CSV
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
    """
    Custom Zipline bundle to ingest data.
    """
    # Generate CSV
    data = generate_csv_from_db()

    # Map all symbols to integer asset IDs
    data = generate_asset_mapping(data)

    # Define the data generator for Zipline
    def data_generator():
        for sid, df in data.groupby("sid"):
            yield sid, df

    # Use `maybe_show_progress` explicitly for iteration
    wrapped_generator = maybe_show_progress(data_generator(), show_progress=show_progress)

    # Write daily bar data
    daily_bar_writer.write((sid, df) for sid, df in wrapped_generator)


# Register the custom bundle
register("custom_csv", custom_bundle)

if __name__ == "__main__":
    try:
        os.environ["ZIPLINE_ROOT"] = "data/zipline_root"
        ingest("custom_csv")
    except Exception as e:
        print(f"Error during ingestion: {e}")
        raise
