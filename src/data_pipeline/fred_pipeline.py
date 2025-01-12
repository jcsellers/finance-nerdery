import logging

import pandas as pd
from fredapi import Fred

from src.utils.sqlite_utils import save_to_sqlite

logger = logging.getLogger(__name__)


class FredPipeline:
    def __init__(self, api_key):
        """
        Initialize the FredPipeline with the API key.

        Args:
            api_key (str): The FRED API key for fetching data.
        """
        self.api_key = api_key
        if not self.api_key or len(self.api_key) != 32:
            raise ValueError("Invalid FRED API key provided.")
        self.fred = Fred(api_key=self.api_key)

    def fetch_data(self, tickers, start_date, end_date):
        """
        Fetch data from FRED and normalize for processing.

        Args:
            tickers (list): List of tickers to fetch.
            start_date (str): Start date for the data range (YYYY-MM-DD).
            end_date (str): End date for the data range (YYYY-MM-DD).

        Returns:
            pd.DataFrame: Normalized data with columns ['date', 'value', 'ticker', 'data_flag'].
        """
        all_data = []
        for ticker in tickers:
            for attempt in range(3):  # Retry mechanism
                try:
                    logger.info(
                        f"Fetching FRED data for ticker: {ticker}, attempt {attempt + 1}"
                    )
                    series = self.fred.get_series(
                        ticker, observation_start=start_date, observation_end=end_date
                    )

                    if series.empty:
                        logger.warning(f"No data fetched for ticker: {ticker}")
                        break

                    data = series.reset_index()
                    data.columns = ["date", "value"]
                    data["ticker"] = ticker
                    data["data_flag"] = "actual"

                    all_data.append(data)
                    logger.info(f"Data fetched successfully for ticker: {ticker}")
                    break
                except Exception as e:
                    logger.error(
                        f"Error fetching FRED data for ticker {ticker} on attempt {attempt + 1}: {e}"
                    )
                    if attempt == 2:
                        logger.warning(
                            f"Skipping ticker {ticker} due to persistent issues."
                        )

        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            logger.info("FRED data fetched successfully for all tickers.")
            return combined_data
        else:
            logger.warning("No data fetched for any tickers.")
            raise ValueError("Failed to fetch data for all provided tickers.")

    def save_data(self, db_path, table_name, dataframe):
        """
        Save the fetched data to SQLite and CSV.

        Args:
            db_path (str): Path to the SQLite database.
            table_name (str): Name of the SQLite table to save data to.
            dataframe (pd.DataFrame): DataFrame containing FRED data.
        """
        try:
            save_to_sqlite(db_path, table_name, dataframe)
            csv_path = f"data/csv_files/{table_name}.csv"
            dataframe.to_csv(csv_path, index=False)
            logger.info(
                f"Data saved to SQLite table '{table_name}' and CSV at '{csv_path}'"
            )
        except Exception as e:
            logger.error(f"Error saving FRED data to {table_name}: {e}")
            raise
