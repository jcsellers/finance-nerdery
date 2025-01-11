import os
import sqlite3
import pandas as pd
from zipline.data.bundles import register, ingest
from zipline.utils.cli import maybe_show_progress

# Paths and configurations
DB_PATH = os.getenv("DB_PATH", "data/output/aligned_data.db")
CSV_PATH = os.getenv("TEMP_CSV_PATH", "data/output/zipline_temp_data.csv")
ZIPLINE_ROOT = os.getenv("ZIPLINE_ROOT", "data/zipline_root")

os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
os.makedirs(ZIPLINE_ROOT, exist_ok=True)


def generate_csv_from_db():
    """
    Generate a CSV file from the SQLite database for ingestion.
    """
    print(f"DB_PATH: {DB_PATH}")
    print(f"CSV_PATH: {CSV_PATH}")

    # Connect to the database
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
        # Fetch data from the database
        data_df = pd.read_sql_query(query, connection)
        print(f"Raw data fetched from the database:\n{data_df.head()}")
        print(f"DataFrame columns: {data_df.columns.tolist()}")
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise
    finally:
        connection.close()

    # Convert date to datetime and drop invalid rows
    data_df["date"] = pd.to_datetime(data_df["date"], errors="coerce")
    data_df = data_df.dropna(subset=["date"])
    print(f"Data after dropping null dates:\n{data_df.head()}")

    # Validate and filter out placeholder dates (e.g., 1970-01-01)
    valid_start_date = pd.Timestamp("1990-01-02")
    data_df = data_df[data_df["date"] >= valid_start_date]
    print(f"Data after filtering placeholder dates:\n{data_df.head()}")

    # Write the validated data to CSV
    try:
        data_df.to_csv(CSV_PATH, index=False)
        print(f"CSV successfully written to {CSV_PATH}")
    except Exception as e:
        print(f"Error writing CSV: {e}")
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
    """
    Custom Zipline bundle to ingest data from the generated CSV.
    """
    generate_csv_from_db()

    # Read and validate the CSV
    try:
        data_df = pd.read_csv(CSV_PATH, parse_dates=["date"])
        print(f"Initial data rows: {len(data_df)}")
        print(f"Initial data:\n{data_df.head()}")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        raise

    # Additional validation and logging
    db_start_date = pd.Timestamp("1990-01-02")
    db_end_date = pd.Timestamp("2025-01-03")
    invalid_dates = data_df[
        (data_df["date"] < db_start_date) | (data_df["date"] > db_end_date)
    ]
    if not invalid_dates.empty:
        print(f"Invalid dates found:\n{invalid_dates}")
        data_df = data_df[~data_df["date"].isin(invalid_dates["date"])]
        print(f"Data after removing invalid dates:\n{data_df.head()}")

    print(f"Validated data rows: {len(data_df)}")

    # Group data by sid and write it to daily_bar_writer
    def data_generator():
        for sid, group in data_df.groupby("sid"):
            yield sid, group

    try:
        daily_bar_writer.write(
            data_generator(),
            show_progress=show_progress,
        )
    except Exception as e:
        print(f"Error during data ingestion: {e}")
        raise


# Register the custom bundle
register("custom_csv", custom_bundle)

if __name__ == "__main__":
    os.environ["ZIPLINE_ROOT"] = ZIPLINE_ROOT
    print(f"ZIPLINE_ROOT: {ZIPLINE_ROOT}")
    try:
        ingest("custom_csv")
    except Exception as e:
        print(f"Error during ingestion: {e}")
        raise
