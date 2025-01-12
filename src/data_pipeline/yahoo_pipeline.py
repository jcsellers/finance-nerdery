import pandas as pd
import yfinance as yf

from src.utils.logger import get_logger

logger = get_logger(__name__)  # Initialize logger


class YahooPipeline:
    def fetch_data(self, tickers, start_date, end_date):
        try:
            data = yf.download(
                tickers, start=start_date, end=end_date, group_by="ticker"
            )
            if isinstance(data, pd.DataFrame):
                data.reset_index(inplace=True)  # Ensure Date is a column
            logger.info("Yahoo Finance data fetched successfully.")
            return data
        except Exception as e:
            logger.error(f"Error fetching Yahoo Finance data: {e}")
            raise
