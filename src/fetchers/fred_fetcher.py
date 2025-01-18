import json
import logging
import os

import pandas as pd
from dotenv import load_dotenv
from fredapi import Fred

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fred_fetcher")

# Load environment variables from the .env file
load_dotenv()


import logging
import os
from urllib.error import URLError

import pandas as pd
from fredapi import Fred

logger = logging.getLogger("fred_fetcher")


class FredFetcher:
    def __init__(self, config_path="config/config.json"):
        self.config_path = config_path
        self.api_key = os.getenv("FRED_API_KEY")
        if not self.api_key:
            raise ValueError(
                "FRED API key not found. Please set FRED_API_KEY in your .env file."
            )
        self.fred = Fred(api_key=self.api_key)

    def fetch_data(self):
        """
        Fetch data for all FRED tickers and return as a DataFrame.
        """
        from urllib.error import URLError

        fred_data = []

        tickers, aliases = self._load_config()

        for ticker in tickers:
            try:
                logger.info(f"Fetching FRED data for: {ticker}")
                data = self.fred.get_series(ticker)
                alias = aliases.get(ticker, ticker)
                df = pd.DataFrame(data, columns=["value"])
                df["date"] = df.index
                df["fred_ticker"] = ticker
                df["alias"] = alias
                fred_data.append(df)
            except URLError as e:
                logger.error(f"Failed to fetch FRED data for {ticker}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error fetching FRED data for {ticker}: {e}")

        if not fred_data:
            logger.warning("No data fetched for FRED tickers.")
            return pd.DataFrame()

        combined_data = pd.concat(fred_data, ignore_index=True)
        return combined_data

    def _load_config(self):
        """
        Load FRED tickers and aliases from the config file.
        """
        import json

        with open(self.config_path, "r") as f:
            config = json.load(f)
        return config["tickers"]["FRED"], config["aliases"]["FRED"]

    @staticmethod
    def clean_data(data):
        """
        Perform cleaning operations on the combined DataFrame.
        """
        # Ensure column names are lowercase and stripped of whitespace
        data.columns = [col.lower().strip() for col in data.columns]
        # Remove duplicate rows
        data = data.drop_duplicates()
        return data


# Example Usage
if __name__ == "__main__":
    # Initialize the fetcher using the config file
    fetcher = FredFetcher(config_path="config/config.json")

    # Fetch and save FRED data
    fred_data = fetcher.fetch_data()
    if not fred_data.empty:
        output_path = "data/output/fred_data.csv"
        fred_data.to_csv(output_path, index=False)
        logger.info(f"FRED data saved to {output_path}.")
