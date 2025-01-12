import pandas as pd
from fredapi import Fred

from src.utils.logger import get_logger

logger = get_logger(__name__)


class FredPipeline:
    def __init__(self, api_key):
        self.fred = Fred(api_key=api_key)

    def fetch_data(self, tickers, start_date, end_date):
        data = {}
        try:
            for ticker in tickers:
                series = self.fred.get_series(
                    ticker, observation_start=start_date, observation_end=end_date
                )
                data[ticker] = series
            df = pd.DataFrame(data)
            logger.info("FRED data fetched successfully.")
            return df
        except Exception as e:
            logger.error(f"Error fetching FRED data: {e}")
            raise
