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
    # Update default path to the correct location
    config_path = os.getenv(
        "CONFIG_PATH", "./config/config.json"
    )  # Adjusted default path
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


def handle_missing_values(df, column_name="Value"):
    """Handle missing values in the data and flag them."""
    df["data_flag"] = "actual"
    if df[column_name].isnull().any():
        # Forward-fill missing values
        df[column_name] = df[column_name].ffill()

        # Mark rows with filled values
        df.loc[df[column_name].isnull(), "data_flag"] = "filled"

    return df


def normalize_column_names(df):
    """Ensure all column names are lowercase for compatibility."""
    df.columns = [col.lower() for col in df.columns]
    return df


def run_yahoo_pipeline(config, csv_dir, sqlite_path):
    """Run Yahoo Finance pipeline and save outputs."""
    try:
        yahoo_pipeline = YahooPipeline()
        yahoo_data = exponential_backoff_retry(
            lambda: yahoo_pipeline.fetch_data(
                tickers=config["tickers"]["Yahoo Finance"],
                start_date=config["date_ranges"]["start_date"],
                end_date=config["date_ranges"]["end_date"],
            )
        )
        yahoo_data = normalize_column_names(yahoo_data)
        yahoo_data.to_csv(os.path.join(csv_dir, "yahoo_data.csv"), index=False)
        save_to_sqlite(sqlite_path, "yahoo_data", yahoo_data)
        logging.info("Yahoo Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Error in Yahoo Pipeline: {e}")


def run_fred_pipeline(config, csv_dir, sqlite_path):
    """Run FRED pipeline and save outputs."""
    try:
        fred_api_key = os.getenv("FRED_API_KEY")
        if not fred_api_key:
            raise ValueError("FRED_API_KEY environment variable is not set.")
        fred_pipeline = FredPipeline(api_key=fred_api_key)
        fred_data = exponential_backoff_retry(
            lambda: fred_pipeline.fetch_data(
                tickers=config["tickers"]["FRED"],
                start_date=config["date_ranges"]["start_date"],
                end_date=config["date_ranges"]["end_date"],
            )
        )
        fred_data = handle_missing_values(fred_data)
        fred_data = normalize_column_names(fred_data)
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
        syn_linear = synthetic_pipeline.generate_linear(
            start_date=config["date_ranges"]["start_date"],
            end_date=config["date_ranges"]["end_date"],
            start_value=1.0,
            growth_rate=0.01,
        )
        syn_cash = normalize_column_names(syn_cash)
        syn_linear = normalize_column_names(syn_linear)
        syn_cash.to_csv(os.path.join(csv_dir, "synthetic_cash.csv"), index=False)
        syn_linear.to_csv(os.path.join(csv_dir, "synthetic_linear.csv"), index=False)
        save_to_sqlite(sqlite_path, "synthetic_cash", syn_cash)
        save_to_sqlite(sqlite_path, "synthetic_linear", syn_linear)
        logging.info("Synthetic Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Error in Synthetic Pipeline: {e}")


def main():
    logging.info("Starting the pipeline...")

    # Load configuration
    config = load_config_from_env()

    # Output paths
    sqlite_path = config["storage"]["SQLite"]
    csv_dir = config["storage"]["CSV"]

    logging.info(f"Ensuring output directories exist: {sqlite_path}, {csv_dir}")
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    os.makedirs(csv_dir, exist_ok=True)

    # Run individual pipelines
    run_yahoo_pipeline(config, csv_dir, sqlite_path)
    run_fred_pipeline(config, csv_dir, sqlite_path)
    run_synthetic_pipeline(config, csv_dir, sqlite_path)

    logging.info("Pipeline execution completed successfully!")


if __name__ == "__main__":
    main()
