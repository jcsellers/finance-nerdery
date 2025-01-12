import logging
from time import sleep

import pandas as pd
import yfinance as yf

from ..utils.sqlite_utils import save_to_sqlite

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class YahooPipeline:
    def __init__(self, retry_attempts=3):
        """
        Initialize the YahooPipeline with retry attempts for fetching data.

        Args:
            retry_attempts (int): Number of retry attempts for data fetching.
        """
        self.retry_attempts = retry_attempts

    def fetch_data(self, tickers, start_date, end_date):
        """
        Fetch data from Yahoo Finance and normalize it.

        Args:
            tickers (list): List of tickers to fetch data for.
            start_date (str): Start date for the data range (YYYY-MM-DD).
            end_date (str): End date for the data range (YYYY-MM-DD).

        Returns:
            pd.DataFrame: Combined and normalized data for all tickers.
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

                    # Normalize data
                    data = data.reset_index()  # Ensure Date column is available
                    data.columns = [
                        str(col).lower() for col in data.columns
                    ]  # Normalize column names
                    data.rename(columns={"adj close": "close"}, inplace=True)
                    data["ticker"] = ticker
                    all_data.append(data)

                    logger.info(f"Data fetched successfully for ticker: {ticker}")
                    break
                except Exception as e:
                    logger.error(
                        f"Error fetching data for ticker {ticker} on attempt {attempt + 1}: {e}"
                    )
                    if attempt < self.retry_attempts - 1:
                        sleep(2**attempt)
                    else:
                        logger.warning(
                            f"Skipping ticker {ticker} after {self.retry_attempts} attempts."
                        )

        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            logger.info("Yahoo Finance data fetched and normalized successfully.")
            return combined_data
        else:
            raise ValueError("No data fetched for the provided tickers.")

    def save_data(self, db_path, table_name, dataframe):
        """
        Save the fetched data to SQLite database.

        Args:
            db_path (str): Path to the SQLite database.
            table_name (str): Name of the SQLite table.
            dataframe (pd.DataFrame): Data to be saved.
        """
        try:
            save_to_sqlite(db_path, table_name, dataframe)
            logger.info(f"Data saved to table '{table_name}' in '{db_path}'.")
        except Exception as e:
            logger.error(f"Error saving data to SQLite: {e}")
            raise
