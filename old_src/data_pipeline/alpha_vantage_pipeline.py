import logging
import time

import pandas as pd
import yfinance as yf
from alpha_vantage.timeseries import TimeSeries
from utils.sqlite_utils import (
    clean_column_names,
    ensure_table_schema,
    save_to_db,
    validate_date_ranges,
)

# Configure logging
logger = logging.getLogger("alpha_vantage_pipeline")


class AlphaVantagePipeline:
    def __init__(
        self,
        api_key,
        tickers,
        start_date,
        end_date,
        db_path,
        max_retries=3,
        backoff_factor=2,
    ):
        """
        Initialize the Alpha Vantage pipeline.

        Args:
            api_key (str): API key for Alpha Vantage.
            tickers (list): List of ticker symbols to fetch data for.
            start_date (str): Start date for fetching data.
            end_date (str): End date for fetching data.
            db_path (str): Path to the SQLite database.
            max_retries (int): Maximum number of retries for API requests.
            backoff_factor (int): Backoff multiplier for retries.
        """
        self.api_key = api_key
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.db_path = db_path
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.alpha_ts = TimeSeries(key=api_key, output_format="pandas")
        self.rate_limit_exceeded = False  # Track API rate limit status

    def fetch_data(self, ticker):
        """
        Fetch data for a single ticker using Alpha Vantage, with fallback to Yahoo Finance.

        Args:
            ticker (str): The ticker symbol to fetch data for.

        Returns:
            pd.DataFrame: The fetched data, or an empty DataFrame if an error occurs.
        """
        if self.rate_limit_exceeded:
            logger.warning(
                f"Skipping Alpha Vantage for {ticker} due to rate limit. Falling back to Yahoo Finance."
            )
            return self.fetch_from_yahoo(ticker)

        logger.info(f"Fetching Alpha Vantage data for ticker: {ticker}")
        retries = 0
        while retries < self.max_retries:
            try:
                # Fetch data from Alpha Vantage
                data, _ = self.alpha_ts.get_daily(symbol=ticker, outputsize="full")
                if data.empty:
                    logger.warning(
                        f"No data returned for ticker: {ticker} (Alpha Vantage)."
                    )
                    return pd.DataFrame()

                # Reset index and ensure 'date'
                data.reset_index(inplace=True)
                data.rename(columns={"index": "date"}, inplace=True)
                data["date"] = pd.to_datetime(data["date"], errors="coerce")
                data.dropna(subset=["date"], inplace=True)

                # Add ticker symbol
                data["symbol"] = ticker
                return clean_column_names(data)
            except Exception as e:
                error_message = str(e)
                logger.error(
                    f"Alpha Vantage failed for ticker {ticker}: {error_message}"
                )

                # Detect API rate limit error and stop further requests
                if (
                    "Our standard API rate limit is 25 requests per day"
                    in error_message
                ):
                    logger.critical(
                        "Alpha Vantage API rate limit exceeded. Falling back to Yahoo Finance."
                    )
                    self.rate_limit_exceeded = True
                    return self.fetch_from_yahoo(ticker)

                retries += 1
                time.sleep(self.backoff_factor**retries)  # Exponential backoff

        logger.error(
            f"Max retries reached for Alpha Vantage (ticker: {ticker}). Falling back to Yahoo Finance."
        )
        return self.fetch_from_yahoo(ticker)

    def fetch_from_yahoo(self, ticker):
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
                # Fetch data from Yahoo Finance
                data = yf.download(
                    ticker, start=self.start_date, end=self.end_date, progress=False
                )
                if data.empty:
                    logger.warning(
                        f"No data returned for ticker: {ticker} (Yahoo Finance)."
                    )
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

                # Ensure 'symbol' column exists for compatibility
                data["symbol"] = ticker

                # Normalize column names
                data = clean_column_names(data)

                return data
            except Exception as e:
                logger.error(f"Yahoo Finance failed for ticker {ticker}: {e}")
                retries += 1
                time.sleep(self.backoff_factor**retries)  # Exponential backoff

        logger.error(f"Max retries reached for Yahoo Finance (ticker: {ticker}).")
        return pd.DataFrame()

    def run(self, table_name="alpha_vantage_prices"):
        """
        Run the Alpha Vantage pipeline with Yahoo Finance fallback.

        Args:
            table_name (str): The database table name to save the data to.
        """
        logger.info("Starting Alpha Vantage pipeline with Yahoo Finance fallback.")

        # Ensure the database schema matches the expected structure
        ensure_table_schema(self.db_path, table_name)

        all_data = []
        for ticker in self.tickers:
            # Fetch data from the primary source or fallback
            data = self.fetch_data(ticker)

            if not data.empty:
                # Validate and save data
                data = validate_date_ranges(data, self.start_date, self.end_date)
                ensure_table_matches_data(self.db_path, table_name, data)
                save_to_db(data, table_name, self.db_path, if_exists="append")
                logger.info(f"Data for ticker {ticker} saved successfully.")

        logger.info("Alpha Vantage pipeline with fallback completed successfully.")
