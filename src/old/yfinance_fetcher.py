import logging

import pandas as pd
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logger = logging.getLogger(__name__)


class YahooFinanceFetcher:
    def __init__(self, missing_data_handling="interpolate"):
        """
        Initialize the YahooFinanceFetcher.

        :param missing_data_handling: Strategy for handling missing data ('interpolate', 'forward_fill', or 'flag').
        """
        self.missing_data_handling = missing_data_handling

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_data(self, ticker, start_date, end_date):
        """
        Fetch data from Yahoo Finance and transform into OHLCV format.

        :param ticker: Yahoo Finance ticker symbol.
        :param start_date: Start date for the data fetch.
        :param end_date: End date for the data fetch.
        :return: DataFrame in OHLCV format.
        """
        logger.info(f"Fetching data for {ticker} from {start_date} to {end_date}.")
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if df.empty:
                logger.warning(f"No data returned for ticker {ticker}.")
                return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
            df = df[["Open", "High", "Low", "Close", "Volume"]]
        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {e}", exc_info=True)
            raise RuntimeError(f"Error fetching data for {ticker}: {e}")

        # Handle missing data
        logger.info(
            f"Handling missing data with strategy: {self.missing_data_handling}"
        )
        if self.missing_data_handling == "interpolate":
            df = df.interpolate()
        elif self.missing_data_handling == "forward_fill":
            df = df.ffill()
        elif self.missing_data_handling == "flag":
            pass  # Retain missing values
        else:
            raise ValueError(
                f"Unsupported missing_data_handling: {self.missing_data_handling}"
            )

        logger.info(f"Successfully fetched data for {ticker} with {len(df)} rows.")
        return df
