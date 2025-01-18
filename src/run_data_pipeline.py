import json
import logging
import os
from datetime import datetime

from dotenv import load_dotenv

from fetch_fred_data import fetch_fred_data
from setup_database import setup_database
from synthetic_pipeline import SyntheticPipeline

# Define the log directory and file
log_dir = "../logs"
log_file = os.path.join(log_dir, "pipeline.log")

# Ensure the log directory exists
os.makedirs(log_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)


def load_config(config_path):
    """Load configuration from a JSON file."""
    with open(config_path, "r") as config_file:
        return json.load(config_file)


def initialize_database(db_path):
    """Initialize the SQLite database and create necessary tables."""
    schemas = {
        "economic_data": """
            CREATE TABLE IF NOT EXISTS economic_data (
                Date DATETIME,
                value REAL,
                symbol TEXT,
                alias TEXT,
                source TEXT,
                is_filled BOOLEAN
            );
        """,
        "historical_data": """
            CREATE TABLE IF NOT EXISTS historical_data (
                Date DATETIME,
                Open REAL,
                High REAL,
                Low REAL,
                Close REAL,
                Adjusted_Close REAL,
                Volume INTEGER,
                ticker TEXT,
                source TEXT DEFAULT 'yfinance',
                is_filled BOOLEAN DEFAULT 0
            );
        """,
        "synthetic_data": """
            CREATE TABLE IF NOT EXISTS synthetic_data (
                Date DATETIME,
                value REAL,
                symbol TEXT,
                source TEXT
            );
        """,
    }
    setup_database(db_path, schemas)


def run_synthetic_pipeline(config, db_path):
    """Run the SyntheticPipeline based on the provided configuration."""
    synthetic_config = config.get("synthetic", {})
    start_date = synthetic_config.get("start_date", "2023-01-01")
    end_date = synthetic_config.get("end_date", "2023-12-31")
    cash_settings = synthetic_config.get("cash_settings", {"start_value": 100.0})
    linear_settings = synthetic_config.get(
        "linear_settings", {"start_value": 100.0, "growth_rate": 1.0}
    )

    pipeline = SyntheticPipeline(
        start_date, end_date, cash_settings, linear_settings, None
    )

    cash_data = pipeline.generate_cash()
    linear_data = pipeline.generate_linear()

    import sqlite3

    with sqlite3.connect(db_path) as conn:
        try:
            cash_data.to_sql("synthetic_data", conn, if_exists="append", index=False)
            linear_data.to_sql("synthetic_data", conn, if_exists="append", index=False)
            logging.info("Synthetic data saved to the database successfully.")
        except Exception as e:
            logging.error(f"Error saving synthetic data to the database: {e}")


def write_yfinance_to_db(csv_path, db_path):
    """Write YFinance data from CSV to the SQLite database."""
    import sqlite3

    import pandas as pd

    conn = sqlite3.connect(db_path)
    try:
        # Load data from CSV
        data = pd.read_csv(csv_path)

        # Rename columns to match the database schema
        data.rename(
            columns={
                "Adj Close": "Adjusted_Close",
                "Date": "Date",
                "Open": "Open",
                "High": "High",
                "Low": "Low",
                "Close": "Close",
                "Volume": "Volume",
            },
            inplace=True,
        )

        # Handle missing Adjusted_Close by copying Close
        if "Adjusted_Close" not in data.columns:
            logging.warning(
                f"Adjusted_Close missing in {csv_path}. Filling with Close."
            )
            data["Adjusted_Close"] = data["Close"]

        # Add additional required columns if missing
        if "ticker" not in data.columns:
            data["ticker"] = os.path.basename(csv_path).split("_data.csv")[0]
        if "source" not in data.columns:
            data["source"] = "yfinance"
        if "is_filled" not in data.columns:
            data["is_filled"] = 0

        # Debug log the DataFrame structure
        logging.debug(
            f"Processed DataFrame for {csv_path}:\nColumns: {list(data.columns)}\n{data.head()}"
        )

        # Verify that columns align with the schema
        required_columns = [
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "Adjusted_Close",
            "Volume",
            "ticker",
            "source",
            "is_filled",
        ]
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in data: {missing_columns}")

        # Write directly to the database
        data.to_sql("historical_data", conn, if_exists="append", index=False)
        logging.info(
            f"YFinance data from {csv_path} saved to the database successfully."
        )
    except Exception as e:
        logging.error(f"Error saving YFinance data to the database: {e}")
    finally:
        conn.close()


def main():
    """Main function to run the data pipeline."""
    # Load environment variables
    load_dotenv()

    # Retrieve the config path from the environment
    config_path = os.getenv("CONFIG_PATH", "config.json")
    if not config_path or not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Config file not found or not set in .env. Provided path: {config_path}"
        )

    # Load the configuration
    config = load_config(config_path)

    # Fetch data parameters
    tickers_config = config.get("tickers", {})
    start_date = config.get("date_ranges", {}).get("start_date", "2023-01-01")
    end_date = config.get("date_ranges", {}).get("end_date", "current")
    if end_date.lower() == "current":
        end_date = datetime.today().strftime("%Y-%m-%d")

    db_path = config.get("storage", {}).get("SQLite", "./data/finance_data.db")
    intermediate_dir = config.get("storage", {}).get("CSV", "./data")

    # Initialize the database
    initialize_database(db_path)

    if not tickers_config:
        logging.error("No tickers specified in the configuration. Exiting.")
        return

    # Run the synthetic data pipeline
    logging.info("Running synthetic data pipeline")
    run_synthetic_pipeline(config, db_path)

    # Process YFinance data
    for source, tickers in tickers_config.items():
        if source.lower() == "yfinance":
            for ticker in tickers:
                csv_path = os.path.join(intermediate_dir, f"{ticker}_data.csv")
                if os.path.exists(csv_path):
                    write_yfinance_to_db(csv_path, db_path)
                else:
                    logging.warning(f"CSV file for {ticker} not found: {csv_path}")
        elif source.lower() == "fred":
            logging.info(f"Fetching data from FRED for tickers: {tickers}")
            aliases = config.get("aliases", {}).get("FRED", {})
            fetch_fred_data(tickers, start_date, end_date, db_path, aliases)


if __name__ == "__main__":
    main()
