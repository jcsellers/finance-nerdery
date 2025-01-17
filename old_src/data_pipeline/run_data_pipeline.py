import logging
import os

from alpha_vantage_pipeline import AlphaVantagePipeline
from dotenv import load_dotenv
from fred_pipeline import FredPipeline
from synthetic_pipeline import SyntheticPipeline
from utils.sqlite_utils import ensure_table_schema
from yahoo_pipeline import YahooPipeline

# Load environment variables
load_dotenv()
DB_PATH = os.getenv("DB_PATH")
ALPHA_API_KEY = os.getenv("ALPHA_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("run_data_pipeline")

import json


class DataPipeline:
    def __init__(self, config):
        """
        Initialize the data pipeline with configuration.

        Args:
            config (dict): Configuration dictionary containing pipeline parameters.
        """
        self.config = config
        self.db_path = config.get("db_path", DB_PATH)
        self.start_date = config.get("start_date", "2000-01-01")
        self.end_date = config.get("end_date", "2025-01-01")
        self.tickers = config.get("tickers", [])

    def run(self):
        """
        Run the entire data acquisition pipeline.
        """
        logger.info("Starting data acquisition pipelines.")

        # Ensure database schemas
        ensure_table_schema(self.db_path, "alpha_vantage_prices")
        ensure_table_schema(self.db_path, "yahoo_prices")
        ensure_table_schema(self.db_path, "fred_data")
        ensure_table_schema(self.db_path, "synthetic_data")

        # Initialize pipelines
        alpha_pipeline = AlphaVantagePipeline(
            api_key=self.config.get("alpha_api_key", ALPHA_API_KEY),
            tickers=self.tickers,
            start_date=self.start_date,
            end_date=self.end_date,
            db_path=self.db_path,
        )

        yahoo_pipeline = YahooPipeline(
            start_date=self.start_date,
            end_date=self.end_date,
            tickers=self.tickers,
            db_path=self.db_path,
        )

        fred_pipeline = FredPipeline(
            start_date=self.start_date,
            end_date=self.end_date,
            tickers=self.config.get("fred_series", []),
            aliases=self.config.get("fred_aliases", {}),
            api_key=self.config.get("fred_api_key", FRED_API_KEY),
        )

        synthetic_pipeline = SyntheticPipeline(
            start_date=self.start_date,
            end_date=self.end_date,
            cash_settings={"start_value": 1000},
            linear_settings={"start_value": 50, "growth_rate": 1},
        )

        try:
            # Run Alpha Vantage pipeline first
            logger.info("Starting Alpha Vantage pipeline.")
            alpha_pipeline.run(table_name="alpha_vantage_prices")

            # Run Yahoo pipeline for remaining tickers
            remaining_tickers = [
                ticker
                for ticker in self.tickers
                if ticker not in alpha_pipeline.fallback_tickers
            ]
            if remaining_tickers:
                logger.info(
                    f"Starting Yahoo Finance pipeline for remaining tickers: {remaining_tickers}"
                )
                yahoo_pipeline.tickers = (
                    remaining_tickers  # Update YahooPipeline with unprocessed tickers
                )
                yahoo_pipeline.run(table_name="yahoo_prices")
            else:
                logger.info(
                    "All tickers were processed by Yahoo as fallbacks. Skipping standalone Yahoo pipeline."
                )

            # Run FRED pipeline
            logger.info("Starting FRED pipeline.")
            fred_pipeline.run(table_name="fred_data")

            # Run Synthetic pipeline
            logger.info("Starting Synthetic pipeline.")
            synthetic_pipeline.run(table_name="synthetic_data")

            logger.info("Data acquisition pipelines completed.")
        except Exception as e:
            logger.error(f"Pipeline execution error: {e}")


if __name__ == "__main__":
    # Example configuration; customize as needed
    config_path = os.getenv("CONFIG_PATH", "config/config.json")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    with open(config_path, "r") as f:
        config = json.load(f)

    pipeline = DataPipeline(config)
    pipeline.run()
