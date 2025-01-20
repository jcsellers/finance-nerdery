import os
import time

import pandas as pd
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
        retry_count = 3
        retry_delay = 5

        cache_file = os.path.join(
            self.cache_dir, f"{series_id}_{start_date}_{end_date}.csv"
        )
        if os.path.exists(cache_file):
            print(f"Loading cached data for {series_id} from {cache_file}")
            return pd.read_csv(cache_file, index_col="Date", parse_dates=True)

        for attempt in range(retry_count):
            try:
                print(
                    f"Fetching data for series_id: {series_id} (Attempt {attempt + 1})"
                )
                series = self.fred.get_series(
                    series_id, observation_start=start_date, observation_end=end_date
                )
                df = series.reset_index()
                df.columns = ["Date", "Value"]
                df.set_index("Date", inplace=True)

                # Cache the data
                print(f"Caching data to {cache_file}")
                df.to_csv(cache_file)

                print(f"Fetched data for {series_id}:\n{df.head()}")
                return df
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < retry_count - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print("Max retries reached. Raising the error.")
                    raise

    def transform_to_ohlcv(self, df):
        print(f"Transforming data to OHLCV:\n{df.head()}")
        df = df.copy()
        if self.missing_data_handling == "interpolate":
            df = df.interpolate(method="linear", axis=0)
        elif self.missing_data_handling == "flag":
            pass  # Retain missing values
        elif self.missing_data_handling == "forward_fill":
            df = df.ffill(axis=0)
        else:
            raise ValueError(
                f"Unsupported missing_data_handling: {self.missing_data_handling}"
            )

        ohlcv = pd.DataFrame(
            {
                "Open": df["Value"],
                "High": df["Value"],
                "Low": df["Value"],
                "Close": df["Value"],
                "Volume": [0] * len(df),
            },
            index=df.index,
        )
        print(f"OHLCV data:\n{ohlcv.head()}")
        return ohlcv

    def save_to_csv(self, ohlcv, file_path):
        print(f"Saving OHLCV data to {file_path}")
        ohlcv.to_csv(file_path)
