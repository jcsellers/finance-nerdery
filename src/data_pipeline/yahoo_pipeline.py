from time import sleep

import pandas as pd
import yfinance as yf

from ..utils.logger import get_logger
from ..utils.sqlite_utils import save_to_sqlite

logger = get_logger(__name__)


class YahooPipeline:
    def __init__(self, retry_attempts=3):
        """
        Initialize the YahooPipeline with configurable retry attempts.

        Args:
            retry_attempts (int): Number of retry attempts for fetching data.
        """
        self.retry_attempts = retry_attempts

    def fetch_data(self, tickers, start_date, end_date):
        """
        Fetch data from Yahoo Finance and normalize for processing.

        Args:
            tickers (list): List of tickers to fetch.
            start_date (str): Start date for the data range (YYYY-MM-DD).
            end_date (str): End date for the data range (YYYY-MM-DD).

        Returns:
            pd.DataFrame: Normalized data with columns ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume'].
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

                    logger.info(f"Data structure for {ticker}:\n{data.head()}")

                    # Normalize MultiIndex DataFrame
                    if isinstance(data.columns, pd.MultiIndex):
                        data = data.stack(level=0).reset_index()
                        data.rename(columns={"Date": "date"}, inplace=True)
                    elif "Open" in data.columns:
                        data.reset_index(inplace=True)
                        data.rename(columns={"Date": "date"}, inplace=True)
                    else:
                        raise ValueError(
                            f"Unexpected data structure for ticker {ticker}"
                        )

                    # Add 'ticker' column only if it doesn't exist
                    if "ticker" not in data.columns:
                        data["ticker"] = ticker

                    # Ensure all column names are lowercase
                    data.columns = [col.lower() for col in data.columns]

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
                        logger.error(
                            f"Failed to fetch data for ticker: {ticker} after {self.retry_attempts} attempts."
                        )

        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)

            # Remove duplicate columns
            combined_data = combined_data.loc[:, ~combined_data.columns.duplicated()]

            logger.info("Yahoo Finance data normalized successfully.")
            logger.info(f"Columns in combined data: {combined_data.columns.tolist()}")
            return combined_data
        else:
            logger.error("No data fetched for any tickers.")
            raise ValueError("Failed to fetch data for all provided tickers.")

    def save_data(self, db_path, table_name, dataframe):
        """
        Save the fetched data to SQLite, ensuring no schema conflicts.

        Args:
            db_path (str): Path to the SQLite database.
            table_name (str): Name of the SQLite table to save data to.
            dataframe (pd.DataFrame): Normalized data to save.
        """
        try:
            # Remove duplicate columns before saving
            dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]

            # Save to SQLite
            save_to_sqlite(db_path, table_name, dataframe)
        except Exception as e:
            logger.error(
                f"Error saving data to SQLite (table: {table_name}, db: {db_path}): {e}"
            )
            raise
