import logging
import os
import time

import pandas as pd
from fredapi import Fred

# Ensure the logs directory exists
log_dir = os.path.join(os.path.dirname(__file__), "../logs")
os.makedirs(log_dir, exist_ok=True)

# Configure logging
log_file_path = os.path.join(log_dir, "fred_fetcher.log")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file_path, mode="w"),  # Log to the logs directory
    ],
)
logger = logging.getLogger(__name__)


class FredFetcher:
    def __init__(self, api_key, cache_dir="cache", missing_data_handling="interpolate"):
        """
        Initialize the FredFetcher.

        :param api_key: API key for FRED API.
        :param cache_dir: Directory to store cached data.
        :param missing_data_handling: Strategy for handling missing data ('interpolate', 'flag', or 'forward_fill').
        """
        logger.info("Initializing FredFetcher...")
        self.api_key = api_key
        self.cache_dir = cache_dir
        self.missing_data_handling = missing_data_handling
        self.fred = Fred(api_key=self.api_key)
        os.makedirs(self.cache_dir, exist_ok=True)
        logger.info("FredFetcher initialized successfully.")

    def fetch_data(self, series_id, start_date, end_date):
        logger.debug(
            f"Entering fetch_data for series_id={series_id}, start_date={start_date}, end_date={end_date}"
        )
        retry_count = 3
        retry_delay = 5

        cache_file = os.path.join(
            self.cache_dir, f"{series_id}_{start_date}_{end_date}.csv"
        )
        if os.path.exists(cache_file):
            logger.info(f"Loading cached data for {series_id} from {cache_file}")
            return pd.read_csv(cache_file, index_col="Date", parse_dates=True)

        for attempt in range(retry_count):
            try:
                logger.info(
                    f"Fetching data for series_id: {series_id} (Attempt {attempt + 1})"
                )
                series = self.fred.get_series(
                    series_id, observation_start=start_date, observation_end=end_date
                )

                if series is None or series.empty:
                    logger.error(f"No data returned for series_id: {series_id}")
                    raise ValueError(
                        f"No data found for series_id: {series_id} in the given date range."
                    )

                df = series.reset_index()
                df.columns = ["Date", "Value"]
                df.set_index("Date", inplace=True)

                # Cache the data
                logger.info(f"Caching data to {cache_file}")
                df.to_csv(cache_file)

                logger.debug(f"Fetched data for {series_id}:\n{df.head()}")
                return df
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}", exc_info=True)
                if attempt < retry_count - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Max retries reached. Raising the error.")
                    raise

    def transform_to_ohlcv(self, df):
        logger.info("Starting data transformation to OHLCV format...")
        logger.debug(f"Initial data:\n{df.head()}")

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

        logger.debug(f"Transformed OHLCV data:\n{ohlcv.head()}")
        logger.info("Data transformation to OHLCV format completed.")
        return ohlcv

    def save_to_csv(self, ohlcv, file_path):
        try:
            logger.info(f"Saving OHLCV data to {file_path}...")
            ohlcv.to_csv(file_path, index_label="Date")
            logger.info(f"Successfully saved OHLCV data to {file_path}")
        except Exception as e:
            logger.error(
                f"Failed to save OHLCV data to {file_path}: {e}", exc_info=True
            )


if __name__ == "__main__":
    # Standalone test for FredFetcher
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        logger.error("FRED_API_KEY is not set in the environment. Exiting.")
    else:
        try:
            fetcher = FredFetcher(api_key)
            logger.info("Testing FRED fetch for series_id='BAMLH0A0HYM2'")
            df = fetcher.fetch_data("BAMLH0A0HYM2", "2023-01-01", "2023-01-10")
            logger.info(f"Fetched data:\n{df.head()}")
        except Exception as e:
            logger.error(f"Standalone test failed: {e}", exc_info=True)
