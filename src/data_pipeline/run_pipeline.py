import json
import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from fred_pipeline import FredPipeline
from synthetic_pipeline import SyntheticPipeline
from yahoo_pipeline import YahooPipeline

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_yahoo_pipeline(start_date, end_date):
    """
    Run the Yahoo data pipeline to fetch and store data.

    Args:
        start_date (str): Start date for data fetching (YYYY-MM-DD).
        end_date (str): End date for data fetching (YYYY-MM-DD).
    """
    yahoo_pipeline = YahooPipeline()
    tickers = config["tickers"]["Yahoo Finance"]
    try:
        logger.info(f"Fetching Yahoo Finance data for {start_date} to {end_date}")
        data = yahoo_pipeline.fetch_data(tickers, start_date, end_date)
        yahoo_pipeline.save_data(config["storage"]["SQLite"], "yahoo_data", data)

        # Save data to CSV
        data.to_csv(f"{config['storage']['CSV']}/yahoo_data.csv", index=False)
        logger.info(
            f"Yahoo data saved to CSV at '{config['storage']['CSV']}/yahoo_data.csv'."
        )

        logger.info("Yahoo Pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in Yahoo Pipeline: {e}")


def run_fred_pipeline(start_date, end_date):
    """
    Run the FRED data pipeline.
    """
    api_key = os.getenv("FRED_API_KEY", config["FRED"]["api_key"])
    if not api_key:
        raise ValueError(
            "FRED_API_KEY is not set in the environment variables or config."
        )

    series_ids = config["tickers"]["FRED"]
    aliases = config["aliases"]["FRED"]

    fred_pipeline = FredPipeline(api_key=api_key)
    try:
        logger.info("Running FRED pipeline.")
        fred_pipeline.run(
            series_ids=series_ids,
            aliases=aliases,
            start_date=start_date,
            end_date=end_date,
            output_dir=config["storage"]["CSV"],
            db_path=config["storage"]["SQLite"],  # Pass db_path here
        )
        logger.info("FRED Pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in FRED Pipeline: {e}")


def run_synthetic_pipeline(start_date, end_date):
    """
    Run the synthetic data pipeline.
    """
    cash_settings = config["Synthetic Data"]["syn_cash"]
    linear_settings = config["Synthetic Data"]["syn_linear"]
    synthetic_pipeline = SyntheticPipeline(
        start_date=start_date,
        end_date=end_date,
        cash_settings=cash_settings,
        linear_settings=linear_settings,
    )
    try:
        logger.info("Running Synthetic pipeline.")
        synthetic_pipeline.save_data(config["storage"]["CSV"])
        synthetic_pipeline.save_to_database(config["storage"]["SQLite"])
        logger.info("Synthetic Pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in Synthetic Pipeline: {e}")


def run_pipeline(start_date="2020-01-01", end_date="2025-12-31"):
    """
    Run the full data pipeline.

    Args:
        start_date (str): Start date for data fetching (YYYY-MM-DD).
        end_date (str): End date for data fetching (YYYY-MM-DD).
    """
    logger.info("Starting full pipeline execution.")
    run_yahoo_pipeline(start_date, end_date)
    run_fred_pipeline(start_date, end_date)
    run_synthetic_pipeline(start_date, end_date)
    logger.info("Full pipeline execution completed successfully.")


if __name__ == "__main__":
    # Load configuration
    with open("config/config.json", "r") as config_file:
        config = json.load(config_file)

    start_date = config["date_ranges"]["start_date"]
    end_date = config["date_ranges"]["end_date"]

    # Convert "current" to today's date
    if end_date == "current":
        end_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Using date range: {start_date} to {end_date}")

    # Run the pipeline
    run_pipeline(start_date, end_date)
