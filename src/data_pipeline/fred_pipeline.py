import pandas as pd
from fredapi import Fred

from ..utils.logger import get_logger

logger = get_logger(__name__)


class FredPipeline:
    def __init__(self, api_key):
        self.fred = Fred(api_key=api_key)

    def fetch_data(self, tickers, start_date, end_date):
        data = {}
        try:
            for ticker in tickers:
                # Fetch series data
                series = self.fred.get_series(
                    ticker, observation_start=start_date, observation_end=end_date
                )
                # Convert to DataFrame and reset index to include Date as a column
                df = series.reset_index()
                df.columns = ["Date", ticker]  # Rename columns
                data[ticker] = df

            # Merge all ticker DataFrames on the Date column
            result = pd.concat(data.values(), axis=1, join="inner")
            result = result.loc[
                :, ~result.columns.duplicated()
            ]  # Remove duplicate Date columns
            logger.info("FRED data fetched successfully.")
            return result
        except Exception as e:
            logger.error(f"Error fetching FRED data: {e}")
            raise
