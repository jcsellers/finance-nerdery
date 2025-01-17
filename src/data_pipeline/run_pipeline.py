import logging
import os

import pandas as pd
from fred_pipeline import FredPipeline
from nasdaq_pipeline import NasdaqPipeline
from synthetic_pipeline import SyntheticPipeline

from utils.sqlite_utils import save_to_db

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main_pipeline")


def run_pipeline(config_path):
    """
    Main function to run the data pipeline.

    Args:
        config_path (str): Path to the configuration file.
    """
    try:
        # Load configuration
        config = pd.read_json(config_path)
        logger.debug(f"Loaded configuration: {config}")

        start_date = config["date_ranges"]["start_date"]
        end_date = config["date_ranges"]["end_date"]
        tickers = config["tickers"].get("Nasdaq", [])

        if not tickers:
            raise KeyError("No tickers found under 'Nasdaq' in configuration.")

        # Fetch Nasdaq API Key from environment variables
        nasdaq_api_key = os.getenv("NASDAQ_API_KEY")
        if not nasdaq_api_key:
            raise EnvironmentError("NASDAQ_API_KEY environment variable not set.")

        logger.info("Starting Nasdaq pipeline.")
        nasdaq_pipeline = NasdaqPipeline(start_date, end_date, tickers, nasdaq_api_key)
        nasdaq_data = nasdaq_pipeline.run()

        if not nasdaq_data.empty:
            save_to_db(nasdaq_data, "nasdaq_data", config["storage"]["SQLite"])

        # FRED Pipeline
        logger.info("Starting FRED pipeline.")
        fred_tickers = config["tickers"]["FRED"]
        fred_pipeline = FredPipeline(start_date, end_date, fred_tickers)
        fred_data = fred_pipeline.run()

        if not fred_data.empty:
            save_to_db(fred_data, "fred_data", config["storage"]["SQLite"])

        # Synthetic Pipeline
        logger.info("Starting Synthetic pipeline.")
        synthetic_pipeline = SyntheticPipeline(start_date, end_date)
        synthetic_data = synthetic_pipeline.run()

        if not synthetic_data.empty:
            save_to_db(synthetic_data, "synthetic_data", config["storage"]["SQLite"])

        logger.info("Pipeline execution completed successfully.")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")


if __name__ == "__main__":
    run_pipeline("config/config.json")
