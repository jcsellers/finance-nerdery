import os
import sqlite3

import pandas as pd
from zipline.data.bundles import ingest, register
from zipline.utils.cli import maybe_show_progress

# Paths and configurations
db_path = os.getenv("DB_PATH", "../.../data/output/aligned_data.db")  # Path to the database
csv_temp_path = os.getenv(
    "TEMP_CSV_PATH", "../data/output/zipline_temp_data.csv"
)  # Temporary CSV path


def generate_csv_from_db(db_path=None, csv_path=None):
    """Generate a CSV file from the SQLite database."""
    db_path = db_path or os.getenv("DB_PATH", "../../data/output/aligned_data.db")
    csv_path = csv_path or os.getenv(
        "TEMP_CSV_PATH", "../../data/output/zipline_temp_data.csv"
    )

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

#.
# Custom Zipline bundle
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
    """Custom data bundle to ingest CSV data."""
    # Generate CSV from database
    generate_csv_from_db()

    # Load the CSV into a DataFrame
    data = pd.read_csv(csv_temp_path, parse_dates=["date"])

    # Ensure column names are compatible
    data = data.rename(
        columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "sid": "sid",
        }
    )

    # Adjust data types
    data["sid"] = data["sid"].astype("category").cat.codes  # Map tickers to integers

    # Group by 'sid' and write daily bar data
    def data_generator():
        for sid, df in data.groupby("sid"):
            yield sid, df

    daily_bar_writer.write(
        data_generator(), show_progress=maybe_show_progress(show_progress)
    )


# Register the custom bundle
register("custom_csv", custom_bundle)

if __name__ == "__main__":
    # Ingest the bundle
    os.environ["ZIPLINE_ROOT"] = "../../data/zipline_root"  # Set Zipline's root directory

    ingest("custom_csv")
