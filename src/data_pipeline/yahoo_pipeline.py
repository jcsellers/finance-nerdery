import logging
import time

import pandas as pd
import yfinance as yf

from utils.sqlite_utils import clean_column_names
from utils.validation import validate_date_ranges

# Configure logging
logger = logging.getLogger("yahoo_pipeline")


class YahooPipeline:
    def __init__(self, start_date, end_date, tickers, max_retries=3, backoff_factor=2):
        self.start_date = start_date
        self.end_date = end_date
        self.tickers = tickers
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def fetch_data(self, ticker):
        logger.info(f"Fetching data for ticker: {ticker}")
        retries = 0
        while retries < self.max_retries:
            try:
                data = yf.download(ticker, start=self.start_date, end=self.end_date)
                if data.empty:
                    logger.warning(f"No data returned for ticker: {ticker}")
                    return pd.DataFrame()

                # Log structure and sample of the fetched data
                logger.debug(
                    f"Fetched data structure for {ticker}: {str(data.info())}\nSample Data:\n{data.head()}"
                )

                # Always reset the index and inspect columns
                data.reset_index(inplace=True)

                # Handle typical date-related columns or use the index as fallback
                potential_date_columns = ["Date", "Datetime"]
                if any(col in data.columns for col in potential_date_columns):
                    date_col = next(
                        (col for col in potential_date_columns if col in data.columns),
                        None,
                    )
                    data.rename(columns={date_col: "date"}, inplace=True)
                else:
                    logger.warning(
                        "No standard 'date' column found; using index as 'date'."
                    )
                    if "index" in data.columns:
                        data.rename(columns={"index": "date"}, inplace=True)
                    else:
                        data.insert(
                            0, "date", pd.to_datetime(data.index, errors="coerce")
                        )

                # Ensure 'date' column exists and is valid
                if "date" not in data.columns:
                    raise ValueError(
                        f"No valid 'date' column found in the data for ticker {ticker}."
                    )

                data["date"] = pd.to_datetime(data["date"], errors="coerce")
                if data["date"].isna().all():
                    raise ValueError(
                        f"All values in 'date' column are invalid for ticker {ticker}."
                    )
                data.dropna(
                    subset=["date"], inplace=True
                )  # Drop rows with invalid dates
                data["symbol"] = ticker
                return data
            except Exception as e:
                logger.error(f"Failed attempt {retries + 1} for ticker {ticker}: {e}")
                retries += 1
                time.sleep(self.backoff_factor**retries)  # Exponential backoff

        logger.error(f"Max retries reached. Could not fetch data for ticker {ticker}.")
        return pd.DataFrame()

    def run(self):
        all_data = []
        for ticker in self.tickers:
            data = self.fetch_data(ticker)
            if not data.empty:
                all_data.append(data)

        if not all_data:
            logger.warning("No data fetched for Yahoo Finance tickers.")
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
        logger.info("Data cleaned and validated for Yahoo Finance.")
        return combined_data
