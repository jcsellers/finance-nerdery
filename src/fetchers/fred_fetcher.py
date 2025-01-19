import json
import logging
import os
from functools import wraps
from time import sleep

import pandas as pd
from fredapi import Fred

# Configure logging
logging.basicConfig(
    filename="./logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("FredFetcher")


def retry_on_failure(retries=3, delay=5):
    """
    Decorator to retry a function in case of failure.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    logger.warning(
                        f"Error: {e}. Retrying {attempts}/{retries} in {delay} seconds...",
                        exc_info=True,
                    )
                    sleep(delay * (2 ** (attempts - 1)))  # Exponential backoff
            logger.error(f"Failed after {retries} attempts.")
            raise Exception(f"Failed after {retries} attempts.")

        return wrapper

    return decorator


class FredFetcher:
    def __init__(self, config_path):
        self.config_path = config_path
        self.api_key = os.getenv("FRED_API_KEY")
        if not self.api_key:
            logger.error(
                "FRED API key not found. Please set FRED_API_KEY in your environment."
            )
            raise ValueError(
                "FRED API key not found. Please set FRED_API_KEY in your environment."
            )
        self.fred = Fred(api_key=self.api_key)

    @retry_on_failure(retries=5, delay=10)
    def fetch_series(self, ticker):
        """
        Fetch a single FRED series with retry logic.
        """
        logger.info(f"Fetching FRED series: {ticker}")
        try:
            data = self.fred.get_series(ticker)
            if data is None or data.empty:
                logger.warning(f"No data returned for {ticker}.")
                return pd.DataFrame()

            data = data.reset_index()
            data.columns = ["date", "value"]
            data["fred_ticker"] = ticker
            return data
        except Exception as e:
            logger.error(f"Error fetching series {ticker}: {e}", exc_info=True)
            raise

    def fetch_data(self, start_date=None):
        """
        Fetch data for all FRED tickers specified in the config file.
        Optionally filter by start_date.
        """
        try:
            tickers, aliases = self._load_config()
        except Exception as e:
            logger.error(f"Failed to load config: {e}", exc_info=True)
            return pd.DataFrame()

        all_data = []
        validation_dir = os.path.join(
            "data/validation_reports"
        )  # Adjust path as needed
        os.makedirs(validation_dir, exist_ok=True)

        for ticker in tickers:
            try:
                series_data = self.fetch_series(ticker)
                if series_data.empty:
                    continue

                alias = aliases.get(ticker, ticker)
                series_data["alias"] = alias

                # Filter by start_date if provided
                if start_date:
                    series_data["date"] = pd.to_datetime(series_data["date"])
                    series_data = series_data[
                        series_data["date"] >= pd.to_datetime(start_date)
                    ]
                    logger.info(
                        f"Filtered data for {ticker} starting from {start_date}."
                    )

                # Validate the data
                validate_data(series_data, validation_dir, ticker)

                all_data.append(series_data)
                logger.info(f"Fetched {len(series_data)} rows for ticker: {ticker}")
            except Exception as e:
                logger.error(f"Failed to fetch data for {ticker}: {e}", exc_info=True)

        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            logger.info(
                f"Fetched data combined into a single DataFrame with {len(combined_data)} rows."
            )
            return combined_data
        else:
            logger.warning("No data fetched for any FRED tickers.")
            return pd.DataFrame()

    def _load_config(self):
        """
        Load FRED tickers and aliases from the config file.
        """
        with open(self.config_path, "r") as f:
            config = json.load(f)
        logger.info("FRED configuration loaded successfully.")
        return config["tickers"]["FRED"], config["aliases"]["FRED"]

    @staticmethod
    def clean_data(data):
        """
        Perform cleaning operations on the combined DataFrame.
        """
        try:
            logger.info("Cleaning fetched data.")
            # Ensure column names are lowercase and stripped of whitespace
            data.columns = [col.lower().strip() for col in data.columns]
            # Remove duplicate rows
            data = data.drop_duplicates()
            logger.info("Data cleaned successfully.")
        except Exception as e:
            logger.error(f"Error during data cleaning: {e}", exc_info=True)
        return data


def validate_data(data, output_dir, name):
    """
    Validate the data and save validation reports.
    """
    logger.info(f"Validating data for {name}...")
    os.makedirs(output_dir, exist_ok=True)

    # Generate summary statistics
    summary = data.describe(include="all")
    summary_path = os.path.join(output_dir, f"{name}_summary.csv")
    summary.to_csv(summary_path)
    logger.info(f"Summary statistics saved to {summary_path}.")

    # Check for missing values
    missing = data.isnull().sum()
    missing_path = os.path.join(output_dir, f"{name}_missing.csv")
    missing.to_csv(missing_path, header=["missing_count"])
    logger.info(f"Missing values report saved to {missing_path}.")

    # Check for anomalies (e.g., zero or negative values in numeric columns)
    anomalies = {}
    for col in data.select_dtypes(include=["number"]).columns:
        anomalies[col] = {
            "zero_values": (data[col] == 0).sum(),
            "negative_values": (data[col] < 0).sum(),
        }
    anomalies_path = os.path.join(output_dir, f"{name}_anomalies.csv")
    pd.DataFrame(anomalies).to_csv(anomalies_path)
    logger.info(f"Anomalies report saved to {anomalies_path}.")
