import json
import logging
import os
from datetime import datetime

from dotenv import load_dotenv

from yfinance_fetcher import fetch_yfinance_data


# Placeholder for future fetchers
def fetch_fred_data(tickers, start_date, end_date, db_path, intermediate_dir):
    # Stub implementation for FRED fetcher
    logging.info(f"Fetching FRED data for tickers: {tickers}")
    pass


def load_config(config_path):
    """Load configuration from a JSON file."""
    with open(config_path, "r") as config_file:
        return json.load(config_file)


def main():
    """Main function to run the data pipeline."""
    # Load environment variables
    load_dotenv()

    # Retrieve the config path from the environment
    config_path = os.getenv("CONFIG_PATH")

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
    db_path = config.get("storage", {}).get("SQLite", "./data/historical_data.db")
    intermediate_dir = config.get("storage", {}).get("CSV", "./data")

    if not tickers_config:
        logger.error("No tickers specified in the configuration. Exiting.")
        return

    # Iterate over data sources and fetch data
    for source, tickers in tickers_config.items():
        try:
            if source.lower() == "yfinance":
                logger.info(f"Fetching data from YFinance for tickers: {tickers}")
                fetch_yfinance_data(
                    tickers, start_date, end_date, db_path, intermediate_dir
                )
            elif source.lower() == "fred":
                logger.info(f"Fetching data from FRED for tickers: {tickers}")
                fetch_fred_data(
                    tickers, start_date, end_date, db_path, intermediate_dir
                )
            else:
                logger.warning(f"Unknown data source: {source}. Skipping.")
        except Exception as e:
            logger.error(f"Failed to fetch data from {source}: {e}")

    logger.info("Data pipeline completed successfully.")


if __name__ == "__main__":
    main()
