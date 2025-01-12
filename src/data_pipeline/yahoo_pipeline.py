import pandas as pd
import yfinance as yf

from ..utils.logger import get_logger

logger = get_logger(__name__)


class YahooPipeline:
    def fetch_data(self, tickers, start_date, end_date):
        """
        Fetch data from Yahoo Finance and normalize for processing.

        Args:
            tickers (list): List of tickers to fetch.
            start_date (str): Start date for the data range (YYYY-MM-DD).
            end_date (str): End date for the data range (YYYY-MM-DD).

        Returns:
            pd.DataFrame: Normalized data with columns ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume'].

        Raises:
            ValueError: If no data is returned for the tickers.
        """
        try:
            data = yf.download(
                tickers, start=start_date, end=end_date, group_by="ticker"
            )
            if data.empty:
                logger.error(f"No data fetched for tickers: {tickers}")
                raise ValueError("Empty response from Yahoo Finance.")

            # Normalize data
            data = data.stack(level=0).reset_index()
            data.rename(columns={"level_1": "Ticker"}, inplace=True)
            logger.info("Yahoo Finance data normalized successfully.")
            return data
        except Exception as e:
            logger.error(f"Error fetching Yahoo Finance data: {e}")
            raise
