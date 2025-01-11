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
        data_df["date"] = pd.to_datetime(data_df["date"], errors="coerce")  # Coerce invalid dates
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


def custom_bundle(environ, asset_db_writer, minute_bar_writer, daily_bar_writer,
                  adjustment_writer, calendar, start_session, end_session, cache,
                  show_progress, output_dir):
    """Custom data bundle to ingest CSV data."""
    generate_csv_from_db()

    # Ensure the CSV file exists
    if not os.path.exists(csv_temp_path):
        raise FileNotFoundError(f"CSV file not found: {csv_temp_path}")

    print(f"CSV file ready for ingestion: {csv_temp_path}")

    # Load the CSV into a DataFrame
    data = pd.read_csv(csv_temp_path, parse_dates=["date"])

    # Filter invalid and out-of-range dates
    data = data.dropna(subset=["date"])  # Drop rows where date parsing failed

    # Fetch the full range of dates from the database
    db_connection = sqlite3.connect(db_path)
    db_date_range = pd.read_sql_query(
        "SELECT MIN(Date) AS start_date, MAX(Date) AS end_date FROM data;", db_connection
    )
    db_connection.close()
    db_start_date = pd.to_datetime(db_date_range["start_date"].iloc[0])
    db_end_date = pd.to_datetime(db_date_range["end_date"].iloc[0])

    print(f"Database date range: {db_start_date} to {db_end_date}")

    # Ensure dates are within the database range
    data = data[(data["date"] >= db_start_date) & (data["date"] <= db_end_date)]

    # Further restrict dates to the simulation range
    data = data[(data["date"] >= start_session) & (data["date"] <= end_session)]

    # Align with valid trading calendar sessions
    valid_sessions = calendar.sessions_in_range(start_session, end_session)
    extra_dates = data[~data["date"].isin(valid_sessions)]  # Identify rows with invalid dates

    # Debugging: Print problematic rows
    if not extra_dates.empty:
        print(f"Extra sessions:\n{extra_dates}")

    # Keep only valid sessions
    data = data[data["date"].isin(valid_sessions)]

    # Debugging: Print summary of valid data
    print(f"Validated data:\n{data.head()}")
    print(f"Total rows after validation: {len(data)}")

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
    data["sid"] = data["sid"].astype("category").cat.codes

    # Final validation: Ensure no invalid dates are passed
    invalid_rows = data[(data["date"] < db_start_date) | (data["date"] > db_end_date)]
    if not invalid_rows.empty:
        print(f"Invalid rows detected:\n{invalid_rows}")
        raise ValueError("Invalid rows detected in final data.")

    # Group by 'sid' and write daily bar data
    def data_generator():
        for sid, df in data.groupby("sid"):
            yield sid, df

    # Pass the generator and `show_progress` directly to `daily_bar_writer.write`
    daily_bar_writer.write(data_generator(), show_progress=show_progress)


# Register the custom bundle
register("custom_csv", custom_bundle)

if __name__ == "__main__":
    os.environ["ZIPLINE_ROOT"] = os.path.abspath("data/zipline_root")
    print(f"ZIPLINE_ROOT: {os.environ['ZIPLINE_ROOT']}")

    # Ingest the bundle
    try:
        ingest("custom_csv")
        print("Bundle ingestion completed successfully.")
    except Exception as e:
        print(f"Error during ingestion: {e}")
        raise
