import logging
import os
import time

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from utils.sqlite_utils import (
    clean_column_names,
    ensure_table_matches_data,
    ensure_table_schema,
    save_to_db,
)

# Load environment variables
load_dotenv()
DB_PATH = os.getenv("DB_PATH")

# Configure logging
logger = logging.getLogger("yahoo_pipeline")


class YahooPipeline:
    def __init__(
        self,
        start_date,
        end_date,
        tickers,
        db_path=DB_PATH,
        max_retries=3,
        backoff_factor=2,
    ):
        """
        Initialize the YahooPipeline.

        Args:
            start_date (str): Start date for fetching data.
            end_date (str): End date for fetching data.
            tickers (list): List of ticker symbols to fetch data for.
            db_path (str): Path to the SQLite database.
            max_retries (int): Maximum number of retries for API requests.
            backoff_factor (int): Backoff multiplier for retries.
        """
        self.start_date = start_date
        self.end_date = end_date
        self.tickers = tickers
        self.db_path = db_path
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def fetch_data(self, ticker):
        """
        Fetch data for a single ticker using Yahoo Finance.

        Args:
            ticker (str): The ticker symbol to fetch data for.

        Returns:
            pd.DataFrame: The fetched data, or an empty DataFrame if an error occurs.
        """
        logger.info(f"Fetching Yahoo Finance data for ticker: {ticker}")
        retries = 0
        while retries < self.max_retries:
            try:
                # Fetch data using yfinance
                data = yf.download(
                    ticker, start=self.start_date, end=self.end_date, progress=False
                )

                # Debug raw data
                logger.debug(f"Raw data fetched for ticker {ticker}:\n{data}")

                if data.empty:
                    logger.warning(f"No data returned for ticker: {ticker}")
                    return pd.DataFrame()

                # Flatten MultiIndex if present
                if isinstance(data.columns, pd.MultiIndex):
                    logger.info(f"Flattening MultiIndex columns for ticker: {ticker}")
                    data.columns = [
                        "_".join(col).strip().lower() for col in data.columns.values
                    ]

                # Reset index and ensure 'date' column exists
                data.reset_index(inplace=True)
                if "Date" in data.columns:
                    data.rename(columns={"Date": "date"}, inplace=True)

                # Add the ticker symbol to the data
                data["symbol"] = ticker

                # Add missing columns if they are not present
                required_columns = [
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "symbol",
                ]
                for col in required_columns:
                    if col not in data.columns:
                        logger.warning(
                            f"Column '{col}' missing for ticker {ticker}. Adding as NaN."
                        )
                        data[col] = None

                # Normalize column names
                data = clean_column_names(data)

                # Check for empty price data
                if data[["open", "close"]].isnull().all(axis=None):
                    logger.warning(
                        f"Ticker {ticker} has no valid price data. Skipping."
                    )
                    return pd.DataFrame()

                logger.debug(f"Final normalized data for {ticker}:\n{data.head()}")
                return data
            except Exception as e:
                logger.error(f"Failed attempt {retries + 1} for ticker {ticker}: {e}")
                retries += 1
                time.sleep(self.backoff_factor**retries)

        logger.error(f"Max retries reached. Could not fetch data for ticker {ticker}.")
        return pd.DataFrame()

    def run(self, table_name="yahoo_prices", exclude_tickers=None):
        """
        Run the Yahoo Finance pipeline for all tickers, excluding processed tickers.

        Args:
            table_name (str): The database table name to save the data to.
            exclude_tickers (list): List of tickers to exclude from processing.
        """
        exclude_tickers = exclude_tickers or []
        remaining_tickers = [t for t in self.tickers if t not in exclude_tickers]

        if not remaining_tickers:
            logger.info("No tickers left to process in Yahoo Finance pipeline.")
            return

        logger.info("Starting Yahoo Finance pipeline.")
        ensure_table_schema(self.db_path, table_name)

        for ticker in remaining_tickers:
            data = self.fetch_data(ticker)
            if not data.empty:
                try:
                    # Save to database directly in Vectorbt format
                    ensure_table_matches_data(self.db_path, table_name, data)
                    save_to_db(data, table_name, self.db_path, if_exists="append")
                    logger.info(f"Data for ticker {ticker} saved successfully.")
                except Exception as e:
                    logger.error(f"Error saving data for ticker {ticker}: {e}")
            else:
                logger.warning(f"No valid data fetched for ticker {ticker}. Skipping.")

        logger.info("Yahoo Finance pipeline completed.")
