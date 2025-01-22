import logging
import os
import time

import pandas as pd
from fredapi import Fred
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logger = logging.getLogger(__name__)


class FredFetcher:
    def __init__(self, api_key, cache_dir="cache", missing_data_handling="interpolate"):
        """
        Initialize the FredFetcher.

        :param api_key: API key for FRED API.
        :param cache_dir: Directory to store cached data.
        :param missing_data_handling: Strategy for handling missing data ('interpolate', 'forward_fill', or 'flag').
        """
        logger.info("Initializing FredFetcher...")
        self.api_key = api_key
        self.cache_dir = cache_dir
        self.missing_data_handling = missing_data_handling
        self.fred = Fred(api_key=self.api_key)
        os.makedirs(self.cache_dir, exist_ok=True)
        logger.info("FredFetcher initialized successfully.")

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_data(self, series_id, start_date, end_date):
        """
        Fetch data from FRED API and return as a DataFrame.

        :param series_id: FRED series ID.
        :param start_date: Start date for the data fetch.
        :param end_date: End date for the data fetch.
        :return: DataFrame with Date as index and Value column.
        """
        logger.info(
            f"Fetching data for series ID {series_id} from {start_date} to {end_date}..."
        )
        cache_file = os.path.join(
            self.cache_dir, f"{series_id}_{start_date}_{end_date}.csv"
        )

        if os.path.exists(cache_file):
            logger.info(
                f"Loading cached data for series ID {series_id} from {cache_file}"
            )
            return pd.read_csv(cache_file, index_col="Date", parse_dates=True)

        try:
            series = self.fred.get_series(
                series_id, observation_start=start_date, observation_end=end_date
            )
            if series.empty:
                logger.warning(f"No data returned for series ID {series_id}.")
                return pd.DataFrame(columns=["Date", "Value"]).set_index("Date")

            df = series.reset_index()
            df.columns = ["Date", "Value"]
            df.set_index("Date", inplace=True)

            logger.info(f"Caching data for series ID {series_id} to {cache_file}")
            df.to_csv(cache_file)

            logger.info(f"Fetched data for series ID {series_id} with {len(df)} rows.")
            return df
        except Exception as e:
            logger.error(
                f"Error fetching data for series ID {series_id}: {e}", exc_info=True
            )
            raise RuntimeError(f"Error fetching data for series ID {series_id}: {e}")

    def transform_to_ohlcv(self, df):
        """
        Transform FRED data to OHLCV format.

        :param df: Input DataFrame with Date index and Value column.
        :return: DataFrame in OHLCV format.
        """
        logger.info("Transforming data to OHLCV format...")
        logger.debug(f"Initial data:\n{df.head()}")

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

        if df.empty or df.isnull().all().all():
            logger.error("Transformed data is empty or all values are NaN.")
            raise ValueError("Transformed data is empty or invalid.")

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

        logger.info(f"Transformed data to OHLCV format with {len(ohlcv)} rows.")
        logger.debug(f"OHLCV data:\n{ohlcv.head()}")
        return ohlcv

    def save_to_csv(self, ohlcv, file_path):
        """
        Save OHLCV data to a CSV file.

        :param ohlcv: DataFrame in OHLCV format.
        :param file_path: Path to save the CSV file.
        """
        try:
            logger.info(f"Saving OHLCV data to {file_path}...")
            ohlcv.to_csv(file_path, index_label="Date")
            logger.info(f"Successfully saved OHLCV data to {file_path}.")
        except Exception as e:
            logger.error(
                f"Failed to save OHLCV data to {file_path}: {e}", exc_info=True
            )
