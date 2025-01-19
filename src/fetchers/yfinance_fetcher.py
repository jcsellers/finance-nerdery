import logging
import os
import time
from functools import wraps

import pandas as pd
import yfinance as yf

# Configure logging
logging.basicConfig(
    filename="./logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("YahooFinanceFetcher")


def retry_on_failure(retries=3, delay=5):
    """
    Decorator to retry a function in case of failure.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    logger.warning(
                        f"Error: {e}. Retrying {attempts}/{retries} in {delay} seconds...",
                        exc_info=True,
                    )
                    time.sleep(delay * (2 ** (attempts - 1)))  # Exponential backoff
            logger.error(f"Failed after {retries} attempts.")
            raise Exception(f"Failed after {retries} attempts.")

        return wrapper

    return decorator


@retry_on_failure(retries=5, delay=10)
def fetch_yfinance_data(ticker, start_date, end_date):
    """
    Fetch historical data from Yahoo Finance for the given ticker.
    """
    logger.info(f"Fetching data for {ticker} from Yahoo Finance...")
    try:
        data = yf.download(ticker, start=start_date, end=end_date)
        if data.empty:
            logger.warning(f"No data retrieved for {ticker} from Yahoo Finance.")
            return pd.DataFrame()

        data = data[["Open", "Close", "Volume"]]
        data.columns = ["open", "close", "volume"]
        data.index = data.index.strftime("%Y-%m-%d")
        logger.info(f"Successfully fetched data for {ticker} ({len(data)} rows).")
        return data
    except Exception as e:
        logger.error(f"Failed to fetch data for {ticker}: {e}", exc_info=True)
        raise


def validate_data(data, output_dir, ticker):
    """
    Validate the data and save reports.
    """
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Generate summary statistics
        summary = data.describe(include="all")
        summary_path = os.path.join(output_dir, f"{ticker}_summary.csv")
        summary.to_csv(summary_path)
        logger.info(f"Summary statistics saved to {summary_path}.")

        # Check for missing values
        missing = data.isnull().sum()
        missing_path = os.path.join(output_dir, f"{ticker}_missing.csv")
        missing.to_csv(missing_path, header=["missing_count"])
        logger.info(f"Missing values report saved to {missing_path}.")

        # Check for anomalies
        anomalies = {}
        for col in data.select_dtypes(include=["number"]).columns:
            anomalies[col] = {
                "zero_values": (data[col] == 0).sum(),
                "negative_values": (data[col] < 0).sum(),
            }
        anomalies_path = os.path.join(output_dir, f"{ticker}_anomalies.csv")
        pd.DataFrame(anomalies).to_csv(anomalies_path)
        logger.info(f"Anomalies report saved to {anomalies_path}.")

    except Exception as e:
        logger.error(f"Error validating data for {ticker}: {e}", exc_info=True)
        raise
