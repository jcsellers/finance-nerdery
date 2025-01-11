import os
import sqlite3
import pandas as pd
from zipline.data.bundles import register, ingest
from zipline.utils.cli import maybe_show_progress

# Paths and configurations
DB_PATH = os.getenv("DB_PATH", "data/output/aligned_data.db")
CSV_PATH = os.getenv("TEMP_CSV_PATH", "data/output/zipline_temp_data.csv")
FRED_PATH = os.getenv("FRED_CSV_PATH", "data/output/fred_data.csv")
ZIPLINE_ROOT = os.getenv("ZIPLINE_ROOT", "data/zipline_root")

os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
os.makedirs(ZIPLINE_ROOT, exist_ok=True)


def fetch_fred_data(connection):
    """
    Fetch FRED data from the database and save it separately.
    """
    query = """
    SELECT
        ticker AS sid,
        Date AS date,
        Open AS open,
        High AS high,
        Low AS low,
        Close AS close,
        Volume AS volume
    FROM data
    WHERE ticker LIKE 'BAMLH%'  -- Assuming all FRED symbols start with a specific prefix
    """
    fred_data = pd.read_sql_query(query, connection)
    fred_data["date"] = pd.to_datetime(fred_data["date"], errors="coerce")
    fred_data = fred_data.dropna(subset=["date"])
    try:
        fred_data.to_csv(FRED_PATH, index=False)
        print(f"FRED data written to {FRED_PATH}")
    except Exception as e:
        print(f"Error writing FRED data: {e}")
        raise
    return fred_data


def generate_csv_from_db():
    """
    Generate a CSV file from the SQLite database for Zipline ingestion.
    """
    print(f"DB_PATH: {DB_PATH}")
    print(f"CSV_PATH: {CSV_PATH}")

    # Connect to the database
    connection = sqlite3.connect(DB_PATH)

    # Fetch equity data
    query = """
    SELECT
        ticker AS sid,
        Date AS date,
        Open AS open,
        High AS high,
        Low AS low,
        Close AS close,
        Volume AS volume
    FROM data
    WHERE ticker NOT LIKE 'BAMLH%'  -- Exclude FRED symbols
    """
    equity_data = pd.read_sql_query(query, connection)
    equity_data["date"] = pd.to_datetime(equity_data["date"], errors="coerce")
    equity_data = equity_data.dropna(subset=["date"])

    # Fetch and save FRED data
    fred_data = fetch_fred_data(connection)

    # Write equity data to CSV
    try:
        equity_data.to_csv(CSV_PATH, index=False)
        print(f"Equity data successfully written to {CSV_PATH}")
    except Exception as e:
        print(f"Error writing equity data: {e}")
        raise

    connection.close()
    return fred_data


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
    fred_data = generate_csv_from_db()

    # Read and validate the CSV for equity data
    equity_data = pd.read_csv(CSV_PATH, parse_dates=["date"])
    print(f"Initial equity data rows: {len(equity_data)}")

    # Group data by sid and write it to daily_bar_writer
    def data_generator():
        for sid, group in equity_data.groupby("sid"):
            yield sid, group

    try:
        daily_bar_writer.write(
            data_generator(),
            show_progress=show_progress,
        )
    except Exception as e:
        print(f"Error during equity data ingestion: {e}")
        raise

    print(f"FRED data available at {FRED_PATH}. Algorithms can load it during runtime.")


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
