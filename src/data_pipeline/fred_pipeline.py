from time import sleep

import pandas as pd
from fredapi import Fred

from ..utils.logger import get_logger

logger = get_logger(__name__)


class FredPipeline:
    def __init__(self, api_key):
        """
        Initialize the FRED pipeline.

        Args:
            api_key (str): API key for accessing FRED data.
        """
        self.fred = Fred(api_key=api_key)

    def fetch_data(self, tickers, start_date, end_date):
        """
        Fetch data from FRED and normalize for processing.

        Args:
            tickers (list): List of FRED tickers to fetch.
            start_date (str): Start date for the data range (YYYY-MM-DD).
            end_date (str): End date for the data range (YYYY-MM-DD).

        Returns:
            pd.DataFrame: Normalized data with columns ['date', 'value', 'ticker', 'data_flag'].
        """
        all_data = []
        for ticker in tickers:
            for attempt in range(3):  # Retry logic with 3 attempts
                try:
                    logger.info(
                        f"Fetching FRED data for ticker: {ticker}, attempt: {attempt + 1}"
                    )
                    series = self.fred.get_series(
                        ticker, observation_start=start_date, observation_end=end_date
                    )

                    if series.empty:
                        logger.warning(f"No data returned for ticker: {ticker}")
                        break

                    # Normalize the fetched series into a DataFrame
                    df = series.reset_index()
                    df.columns = ["date", "value"]
                    df["ticker"] = ticker
                    df["data_flag"] = "actual"

                    all_data.append(df)
                    logger.info(f"Data fetched successfully for ticker: {ticker}")
                    break
                except Exception as e:
                    logger.error(
                        f"Error fetching data for ticker {ticker} on attempt {attempt + 1}: {e}"
                    )
                    if attempt < 2:  # Retry for the first two failures
                        sleep(2**attempt)  # Exponential backoff
                    else:
                        logger.error(
                            f"Failed to fetch data for ticker: {ticker} after 3 attempts."
                        )
                        logger.warning(
                            f"Skipping ticker {ticker} due to persistent issues."
                        )
                        break

        # Combine all fetched data into a single DataFrame
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            logger.info(f"FRED data fetched successfully for {len(all_data)} tickers.")
            return combined_data
        else:
            logger.warning("No data fetched for any tickers.")
            return pd.DataFrame()
