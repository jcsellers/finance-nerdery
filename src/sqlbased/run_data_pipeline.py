import json
import logging
import os
from datetime import datetime

from clean_yfinance import clean_yfinance_csv
from dotenv import load_dotenv
from fetch_fred_data import fetch_fred_data
from setup_database import setup_database
from yfinance_fetcher import fetch_yfinance_data

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
                symbol TEXT,
                source TEXT
            );
        """,
    }
    setup_database(db_path, schemas)


def insert_cleaned_data_to_db(cleaned_file, db_path):
    """Insert cleaned data into the SQLite database."""
    import sqlite3

    import pandas as pd

    conn = sqlite3.connect(db_path)
    try:
        # Load the cleaned data
        df = pd.read_csv(cleaned_file)

        # Insert into the historical_data table
        df.to_sql("historical_data", conn, if_exists="append", index=False)
        logging.info(f"Inserted cleaned data from {cleaned_file} into the database.")
    except Exception as e:
        logging.error(f"Failed to insert data from {cleaned_file}: {e}")
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

    # Configure logging
    log_dir = config.get("storage", {}).get("output_dir", "./logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    logger = logging.getLogger("data_pipeline")

    logger.info("Starting data pipeline")

    # Fetch data parameters
    tickers_config = config.get("tickers", {})
    start_date = config.get("date_ranges", {}).get("start_date", "2023-01-01")
    end_date = config.get("date_ranges", {}).get("end_date", "current")

    # Convert 'current' to today's date
    if end_date.lower() == "current":
        end_date = datetime.today().strftime("%Y-%m-%d")

    db_path = config.get("storage", {}).get("SQLite", "./data/finance_data.db")
    intermediate_dir = config.get("storage", {}).get("CSV", "../data")

    # Initialize the database
    initialize_database(db_path)

    if not tickers_config:
        logger.error("No tickers specified in the configuration. Exiting.")
        return

    # Iterate over data sources and fetch data
    for source, tickers in tickers_config.items():
        try:
            if source.lower() == "yfinance":
                logger.info(f"Fetching data from YFinance for tickers: {tickers}")
                fetch_yfinance_data(
                    tickers=tickers,
                    start_date=start_date,
                    end_date=end_date,
                    intermediate_dir=intermediate_dir,
                )

                # Clean fetched data
                for ticker in tickers:
                    raw_file = os.path.join(intermediate_dir, f"{ticker}_raw.csv")
                    cleaned_file = os.path.join(
                        intermediate_dir, f"{ticker}_cleaned.csv"
                    )
                    if os.path.exists(raw_file):
                        logger.info(f"Cleaning data for ticker: {ticker}")
                        clean_yfinance_csv(raw_file, cleaned_file)

                        # Insert cleaned data into the database
                        insert_cleaned_data_to_db(cleaned_file, db_path)
                    else:
                        logger.warning(
                            f"Raw file for ticker {ticker} not found: {raw_file}"
                        )
            elif source.lower() == "fred":
                logger.info(f"Fetching data from FRED for tickers: {tickers}")
                aliases = config.get("aliases", {}).get("FRED", {})
                fetch_fred_data(tickers, start_date, end_date, db_path, aliases)
            else:
                logger.warning(f"Unknown data source: {source}. Skipping.")
        except Exception as e:
            logger.error(f"Failed to fetch data from {source}: {e}")

    logger.info("Data pipeline completed successfully.")


if __name__ == "__main__":
    main()
