import os

import pandas as pd
import pandas_market_calendars as mcal
from fredapi import Fred


class FredFetcher:
    def __init__(self, api_key, cache_dir="cache", missing_data_handling="interpolate"):
        """
        Initialize the FredFetcher.

        :param api_key: API key for FRED API.
        :param cache_dir: Directory to store cached data.
        :param missing_data_handling: Strategy for handling missing data ('interpolate', 'flag', or 'forward_fill').
        """
        self.api_key = api_key
        self.cache_dir = cache_dir
        self.missing_data_handling = missing_data_handling
        self.fred = Fred(api_key=self.api_key)
        os.makedirs(self.cache_dir, exist_ok=True)

    def fetch_data(self, series_id, start_date, end_date):
        retry_count = 3  # Number of retries
        retry_delay = 5  # Delay in seconds between retries

        for attempt in range(retry_count):
            try:
                # Attempt to fetch the data
                series = self.fred.get_series(
                    series_id, observation_start=start_date, observation_end=end_date
                )
                df = series.reset_index()
                df.columns = ["Date", "Value"]
                df.set_index("Date", inplace=True)
                return df
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for series_id {series_id}: {e}")
                if attempt < retry_count - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print("Max retries reached. Raising the error.")
                    raise

    def transform_to_ohlcv(self, df):
        """
        Transform FRED data into OHLCV format.

        :param df: Input DataFrame with a single "Value" column.
        :return: DataFrame in OHLCV format.
        """
        print(f"Initial DataFrame:\n{df.head()}")
        df = df.copy()
        if self.missing_data_handling == "interpolate":
            df = df.interpolate(method="linear", axis=0)
        elif self.missing_data_handling == "flag":
            pass  # Retain missing values as-is
        elif self.missing_data_handling == "forward_fill":
            df = df.ffill(axis=0)
        else:
            raise ValueError(
                f"Unsupported missing_data_handling: {self.missing_data_handling}"
            )

        print(
            f"DataFrame after missing data handling ({self.missing_data_handling}):\n{df.head()}"
        )

        ohlcv = pd.DataFrame(
            {
                "Open": df["Value"],
                "High": df["Value"],
                "Low": df["Value"],
                "Close": df["Value"],
                "Volume": [0] * len(df),  # Placeholder volume
            },
            index=df.index,
        )

        print(f"Transformed OHLCV DataFrame:\n{ohlcv.head()}")
        return ohlcv
