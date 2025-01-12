import pandas as pd
import yfinance as yf

from ..utils.logger import get_logger

logger = get_logger(__name__)


class YahooPipeline:
    def fetch_data(self, tickers, start_date, end_date):
        """Fetch data from Yahoo Finance and normalize for processing."""
        try:
            # Fetch data for all tickers
            data = yf.download(
                tickers, start=start_date, end=end_date, group_by="ticker"
            )

            # Normalize data
            if isinstance(data, pd.DataFrame):
                data = data.stack(level=0).reset_index()
                data.rename(columns={"level_1": "Ticker"}, inplace=True)
                logger.info("Yahoo Finance data normalized successfully.")

            return data
        except Exception as e:
            logger.error(f"Error fetching Yahoo Finance data: {e}")
            raise
