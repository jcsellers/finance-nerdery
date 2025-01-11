import os
import sqlite3
import pandas as pd
from zipline.data.bundles import register, ingest
from zipline.utils.cli import maybe_show_progress

# Paths and configurations
DB_PATH = os.getenv("DB_PATH", "data/output/aligned_data.db")
CSV_PATH = os.getenv("CSV_PATH", "data/output/zipline_temp_data.csv")


def generate_csv_from_db():
    """
    Generate a CSV file from the SQLite database.
    """
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

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
        data["date"] = pd.to_datetime(data["date"])
        print(f"Raw data fetched from the database:\n{data.head()}")
    except Exception as e:
        raise RuntimeError(f"Error reading data from database: {e}")
    finally:
        connection.close()

    # Drop null dates and filter placeholder rows
    data = data.dropna(subset=["date"])
    data = data[(data["High"] != 0) & (data["Low"] != 0) & (data["Volume"] != 0)]
    print(f"Validated data rows: {len(data)}")

    # Write to CSV
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    data.to_csv(CSV_PATH, index=False)
    print(f"Equity data successfully written to {CSV_PATH}")
    return data


def generate_asset_mapping(data):
    """
    Create a mapping of assets to integer IDs for Zipline ingestion.
    """
    unique_assets = data["sid"].unique()
    asset_map = {asset: idx for idx, asset in enumerate(unique_assets)}
    print(f"Asset mapping: {asset_map}")
    return data, asset_map


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
    # Step 1: Generate CSV
    data = generate_csv_from_db()

    # Step 2: Generate asset mapping
    data, asset_map = generate_asset_mapping(data)

    # Step 3: Create data generator
    def data_generator():
        for sid, df in data.groupby("sid"):
            yield asset_map[sid], df

    # Step 4: Wrap the generator for manual progress tracking
    def progress_generator(generator, total):
        for count, item in enumerate(generator, start=1):
            yield item
            print(f"Progress: {count}/{total} items processed", end="\r")

    total_assets = len(asset_map)
    wrapped_generator = progress_generator(data_generator(), total_assets)

    # Step 5: Validate generator output
    for sid, df in wrapped_generator:
        assert isinstance(df, pd.DataFrame), f"Invalid data for SID {sid}: not a DataFrame"
        print(f"Validated SID: {sid}, DataFrame:\n{df.head()}")

    # Step 6: Write data to daily bar writer
    daily_bar_writer.write(data_generator(), show_progress=None)


# Register the custom bundle
register("custom_csv", custom_bundle)

if __name__ == "__main__":
    # Run the custom bundle ingestion
    os.environ["ZIPLINE_ROOT"] = "data/zipline_root"
    try:
        ingest("custom_csv")
    except Exception as e:
        print(f"Error during ingestion: {e}")
