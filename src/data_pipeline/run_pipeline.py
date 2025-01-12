# Updated to Address Code Nazi Feedback and Examples
import json
import logging
import os
from datetime import datetime
from time import sleep

from dotenv import load_dotenv

from ..utils.sqlite_utils import save_to_sqlite
from .fred_pipeline import FredPipeline
from .synthetic_pipeline import SyntheticPipeline
from .yahoo_pipeline import YahooPipeline

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


def exponential_backoff_retry(func, retries=3, backoff_factor=2):
    """Retry logic with exponential backoff."""
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                sleep(backoff_factor**attempt)
            else:
                raise


def load_config_from_env():
    """Load configuration from environment variable or use default path."""
    config_path = os.getenv("CONFIG_PATH", "./config/config.json")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    return load_config(config_path)


def load_config(config_path):
    """Load and preprocess the configuration file."""
    try:
        with open(config_path, "r") as f:
            config = json.load(f)

        # Replace "current" with today's date
        today = datetime.now().strftime("%Y-%m-%d")
        if config["date_ranges"]["end_date"] == "current":
            config["date_ranges"]["end_date"] = today

        return config
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        raise


def ensure_output_directories(config):
    """Ensure all output directories exist."""
    sqlite_path = config["storage"]["SQLite"]
    csv_dir = config["storage"]["CSV"]
    logging.info(f"Ensuring output directories exist: {sqlite_path}, {csv_dir}")
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    os.makedirs(csv_dir, exist_ok=True)


def run_yahoo_pipeline(config, csv_dir, sqlite_path):
    """Run Yahoo Finance pipeline and save outputs."""
    try:
        yahoo_pipeline = YahooPipeline(retry_attempts=3)
        yahoo_data = exponential_backoff_retry(
            lambda: yahoo_pipeline.fetch_data(
                tickers=config["tickers"]["Yahoo Finance"],
                start_date=config["date_ranges"]["start_date"],
                end_date=config["date_ranges"]["end_date"],
            )
        )
        yahoo_data.to_csv(os.path.join(csv_dir, "yahoo_data.csv"), index=False)
        save_to_sqlite(sqlite_path, "yahoo_data", yahoo_data)
        logging.info("Yahoo Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Error in Yahoo Pipeline: {e}")


def run_fred_pipeline(config, csv_dir, sqlite_path):
    """Run FRED pipeline and save outputs."""
    try:
        dotenv_path = os.path.join(os.path.dirname(__file__), "../../.env")
        load_dotenv(dotenv_path)

        fred_api_key = os.getenv("FRED_API_KEY")
        if not fred_api_key:
            raise ValueError(
                "FRED_API_KEY environment variable is not set. Please configure it in your .env file or environment."
            )
        fred_pipeline = FredPipeline(api_key=fred_api_key)
        fred_data = exponential_backoff_retry(
            lambda: fred_pipeline.fetch_data(
                tickers=config["tickers"]["FRED"],
                start_date=config["date_ranges"]["start_date"],
                end_date=config["date_ranges"]["end_date"],
            )
        )
        fred_data.to_csv(os.path.join(csv_dir, "fred_data.csv"), index=False)
        save_to_sqlite(sqlite_path, "fred_data", fred_data)
        logging.info("FRED Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Error in FRED Pipeline: {e}")


def run_synthetic_pipeline(config, csv_dir, sqlite_path):
    """Run synthetic data pipeline and save outputs."""
    try:
        synthetic_pipeline = SyntheticPipeline()
        syn_cash = synthetic_pipeline.generate_cash(
            start_date=config["date_ranges"]["start_date"],
            end_date=config["date_ranges"]["end_date"],
            start_value=1.0,
        )
        syn_cash.to_csv(os.path.join(csv_dir, "synthetic_cash.csv"), index=False)
        save_to_sqlite(sqlite_path, "synthetic_cash", syn_cash)
        logging.info("Synthetic Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Error in Synthetic Pipeline: {e}")


def main():
    logging.info("Starting the pipeline...")

    try:
        # Load configuration
        config = load_config_from_env()

        # Ensure output directories exist
        ensure_output_directories(config)

        # Run individual pipelines
        run_yahoo_pipeline(
            config, config["storage"]["CSV"], config["storage"]["SQLite"]
        )
        run_fred_pipeline(config, config["storage"]["CSV"], config["storage"]["SQLite"])
        run_synthetic_pipeline(
            config, config["storage"]["CSV"], config["storage"]["SQLite"]
        )
        logging.info("Pipeline execution completed successfully!")
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")


if __name__ == "__main__":
    main()
