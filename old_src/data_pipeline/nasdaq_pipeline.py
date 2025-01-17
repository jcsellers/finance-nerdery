import logging

import nasdaqdatalink as nasdaq_data_link
import pandas as pd
from dotenv import load_dotenv
from utils.sqlite_utils import clean_column_names
from utils.validation import validate_date_ranges

load_dotenv()

# Configure logging
logger = logging.getLogger("nasdaq_pipeline")


class NasdaqPipeline:
    def __init__(self, start_date, end_date, tickers, api_key):
        self.start_date = start_date
        self.end_date = end_date
        self.tickers = tickers
        self.api_key = api_key
        nasdaq_data_link.ApiConfig.api_key = api_key

    def fetch_data(self, ticker):
        logger.info(f"Fetching data for ticker: {ticker}")
        try:
            data = nasdaq_data_link.get(
                f"WIKI/{ticker}", start_date=self.start_date, end_date=self.end_date
            )
            if data.empty:
                logger.warning(f"No data returned for ticker: {ticker}")
                return pd.DataFrame()

            # Reset index to ensure 'date' column
            data.reset_index(inplace=True)
            data.rename(columns={"Date": "date"}, inplace=True)

            # Ensure 'date' column is valid
            data["date"] = pd.to_datetime(data["date"], errors="coerce")
            if data["date"].isna().all():
                raise ValueError(
                    f"All values in 'date' column are invalid for ticker {ticker}."
                )
            data.dropna(subset=["date"], inplace=True)  # Drop rows with invalid dates
            data["symbol"] = ticker
            return data
        except Exception as e:
            logger.error(f"Failed to fetch data for ticker {ticker}: {e}")
            return pd.DataFrame()

    def run(self):
        all_data = []
        for ticker in self.tickers:
            data = self.fetch_data(ticker)
            if not data.empty:
                all_data.append(data)

        if not all_data:
            logger.warning("No data fetched for Nasdaq tickers.")
            return pd.DataFrame()

        # Combine and clean data
        combined_data = pd.concat(all_data, ignore_index=True)
        logger.info("Data combined successfully.")

        # Reset index and ensure column names are strings
        if isinstance(combined_data.index, pd.MultiIndex):
            combined_data = combined_data.reset_index()

        combined_data.columns = [str(col) for col in combined_data.columns]

        # Clean and validate
        combined_data = clean_column_names(combined_data)
        combined_data = validate_date_ranges(
            combined_data, self.start_date, self.end_date
        )
        logger.info("Data cleaned and validated for Nasdaq Data Link.")
        return combined_data
