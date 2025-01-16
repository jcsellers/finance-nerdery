import logging
import sqlite3

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


class YahooPipeline:
    def __init__(self, retry_attempts=3):
        """
        Initialize the Yahoo data pipeline.

        Args:
            retry_attempts (int): Number of retry attempts for fetching data.
        """
        self.retry_attempts = retry_attempts

    def fetch_data(self, tickers, start_date, end_date):
        """
        Fetch data from Yahoo Finance.

        Args:
            tickers (list): List of tickers to fetch.
            start_date (str): Start date for data fetching (YYYY-MM-DD).
            end_date (str): End date for data fetching (YYYY-MM-DD).

        Returns:
            pd.DataFrame: Combined financial data for all tickers.
        """
        all_data = []
        for ticker in tickers:
            for attempt in range(self.retry_attempts):
                try:
                    logger.info(
                        f"Fetching data for ticker: {ticker}, attempt: {attempt + 1}"
                    )
                    data = yf.download(
                        ticker, start=start_date, end=end_date, group_by="ticker"
                    )
                    if data.empty:
                        logger.warning(f"No data fetched for ticker: {ticker}")
                        break
                    data["symbol"] = ticker
                    all_data.append(
                        data.reset_index()
                    )  # Ensure 'date' column is included
                    break
                except Exception as e:
                    logger.error(f"Error fetching data for ticker {ticker}: {e}")
                    if attempt == self.retry_attempts - 1:
                        logger.warning(
                            f"Max retries reached for ticker: {ticker}. Skipping."
                        )
        if not all_data:
            raise ValueError("No data was fetched from Yahoo Finance.")
        return pd.concat(all_data, ignore_index=True)

    def save_data(self, db_path, table_name, data):
        """
        Save data to an SQLite database.

        Args:
            db_path (str): Path to the SQLite database.
            table_name (str): Table name to save data.
            data (pd.DataFrame): Data to save.
        """
        try:
            conn = sqlite3.connect(db_path)
            data.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.close()
            logger.info(
                f"Data saved to SQLite database: {db_path}, table: {table_name}"
            )
        except Exception as e:
            logger.error(f"Error saving data to database: {e}")
