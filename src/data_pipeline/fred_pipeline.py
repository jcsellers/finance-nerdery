import pandas as pd
from fredapi import Fred

from ..utils.logger import get_logger

logger = get_logger(__name__)


class FredPipeline:
    def __init__(self, api_key):
        self.fred = Fred(api_key=api_key)

    def fetch_data(self, tickers, start_date, end_date):
        """Fetch data from FRED and normalize for processing."""
        data = []
        try:
            for ticker in tickers:
                # Fetch series data
                series = self.fred.get_series(
                    ticker, observation_start=start_date, observation_end=end_date
                )
                # Reset index to include Date as a column
                df = series.reset_index()
                df.columns = ["Date", "Value"]
                df["Ticker"] = ticker  # Add Ticker column
                df["data_flag"] = "actual"  # Default flag as "actual"
                data.append(df)

            # Combine all data into a single DataFrame
            result = pd.concat(data, ignore_index=True)
            logger.info("FRED data fetched successfully.")
            return result
        except Exception as e:
            logger.error(f"Error fetching FRED data: {e}")
            raise
