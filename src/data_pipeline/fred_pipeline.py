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
            pd.DataFrame: Normalized data with columns ['Date', 'Ticker', '<ticker>', 'data_flag'].
        """
        data = []
        try:
            for ticker in tickers:
                # Fetch time series data for the ticker
                series = self.fred.get_series(
                    ticker, observation_start=start_date, observation_end=end_date
                )
                if series.empty:
                    raise ValueError(f"No data returned for ticker: {ticker}")

                # Normalize the data
                df = series.reset_index()
                df.columns = ["Date", ticker]
                df["Ticker"] = ticker
                df[
                    "data_flag"
                ] = "actual"  # Indicate that data is directly from the source
                data.append(df)

            # Combine all data into a single DataFrame
            result = pd.concat(data, ignore_index=True)
            logger.info(f"FRED data fetched successfully for {len(tickers)} tickers.")
            return result
        except Exception as e:
            logger.error(f"Error fetching data for ticker {ticker}: {e}")
            raise
