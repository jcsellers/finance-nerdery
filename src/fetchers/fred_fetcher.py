import json
import logging
import os
import time
from urllib.error import URLError

import pandas as pd
from fredapi import Fred

# Configure logging
logging.basicConfig(
    filename="./logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("FredFetcher")


class FredFetcher:
    def __init__(self, config_path="config/config.json"):
        self.config_path = config_path
        self.api_key = os.getenv("FRED_API_KEY")
        if not self.api_key:
            logger.error(
                "FRED API key not found. Please set FRED_API_KEY in your .env file."
            )
            raise ValueError(
                "FRED API key not found. Please set FRED_API_KEY in your .env file."
            )
        self.fred = Fred(api_key=self.api_key)

    def fetch_with_retries(self, ticker, retries=3, delay=2):
        """
        Fetch FRED data with retry logic for network resilience.
        """
        for attempt in range(retries):
            try:
                logger.info(
                    f"Fetching FRED data for ticker: {ticker} (Attempt {attempt + 1})"
                )
                data = self.fred.get_series(ticker)
                return data
            except URLError as e:
                logger.warning(f"Retry {attempt + 1} for ticker {ticker} due to: {e}")
                time.sleep(delay * (2**attempt))  # Exponential backoff
            except Exception as e:
                logger.error(
                    f"Unexpected error for ticker {ticker}: {e}", exc_info=True
                )
                break
        logger.error(
            f"Failed to fetch data for ticker {ticker} after {retries} retries."
        )
        return None

    def fetch_data(self):
        """
        Fetch data for all FRED tickers and return as a DataFrame.
        """
        fred_data = []

        try:
            tickers, aliases = self._load_config()
        except Exception as e:
            logger.error(f"Failed to load config: {e}", exc_info=True)
            return pd.DataFrame()

        for ticker in tickers:
            data = self.fetch_with_retries(ticker)
            if data is None:
                continue

            alias = aliases.get(ticker, ticker)
            df = pd.DataFrame(data, columns=["value"])
            df["date"] = df.index
            df["fred_ticker"] = ticker
            df["alias"] = alias
            fred_data.append(df)
            logger.info(f"Fetched {len(df)} rows for ticker: {ticker}")

        if not fred_data:
            logger.warning("No data fetched for any FRED tickers.")
            return pd.DataFrame()

        combined_data = pd.concat(fred_data, ignore_index=True)
        logger.info(
            f"Fetched data combined into a single DataFrame with {len(combined_data)} rows."
        )
        return combined_data

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


# Example Usage
if __name__ == "__main__":
    try:
        fetcher = FredFetcher(config_path="config/config.json")
        fred_data = fetcher.fetch_data()
        if not fred_data.empty:
            output_path = "data/output/fred_data.csv"
            fred_data.to_csv(output_path, index=False)
            logger.info(f"FRED data saved to {output_path}.")
    except Exception as e:
        logger.error(f"Failed to fetch or save FRED data: {e}", exc_info=True)
