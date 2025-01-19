import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import wraps

import pandas as pd
import yfinance as yf
from tqdm import tqdm  # Added tqdm for progress bar

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


@retry_on_failure(retries=6, delay=5)
def fetch_yfinance_data(ticker, start_date, end_date):
    """
    Fetch historical data from Yahoo Finance for the given ticker.
    """
    logger.info(f"Fetching data for {ticker} from Yahoo Finance...")
    start_time = time.time()  # Log start time
    try:
        data = yf.download(
            ticker, start=start_date, end=end_date, timeout=30
        )  # Increased timeout to 30s
        end_time = time.time()  # Log end time
        logger.info(
            f"Data fetch for {ticker} took {end_time - start_time:.2f} seconds."
        )
        if data.empty:
            logger.warning(f"No data retrieved for {ticker} from Yahoo Finance.")
            return pd.DataFrame()

        # Log columns to debug structure
        logger.debug(f"Columns returned for {ticker}: {data.columns.tolist()}")

        # Dynamically select required columns
        required_columns = {"Open": "open", "Close": "close", "Volume": "volume"}
        selected_columns = {
            col: alias for col, alias in required_columns.items() if col in data.columns
        }

        if not selected_columns:
            logger.error(
                f"Required columns are missing for {ticker}. Available columns: {data.columns.tolist()}"
            )
            return pd.DataFrame()

        data = data[list(selected_columns.keys())]
        data.rename(columns=selected_columns, inplace=True)

        # Ensure proper indexing
        data.index = data.index.strftime("%Y-%m-%d")

        logger.info(f"Successfully fetched data for {ticker} ({len(data)} rows).")
        return data
    except Exception as e:
        logger.error(f"Failed to fetch data for {ticker}: {e}", exc_info=True)
        raise


def validate_data(data, output_dir, name):
    """
    Validate the data and save validation reports.
    """
    logger.info(f"Validating data for {name}...")
    os.makedirs(output_dir, exist_ok=True)

    # Generate summary statistics
    summary = data.describe(include="all")
    summary_path = os.path.join(output_dir, f"{name}_summary.csv")
    summary.to_csv(summary_path)
    logger.info(f"Summary statistics saved to {summary_path}.")

    # Check for missing values
    missing = data.isnull().sum()
    missing_path = os.path.join(output_dir, f"{name}_missing.csv")
    missing.to_csv(missing_path, header=["missing_count"])
    logger.info(f"Missing values report saved to {missing_path}.")

    # Check for anomalies (e.g., zero or negative values in numeric columns)
    anomalies = {}
    for col in data.select_dtypes(include=["number"]).columns:
        anomalies[col] = {
            "zero_values": (data[col] == 0).sum(),
            "negative_values": (data[col] < 0).sum(),
        }
    anomalies_path = os.path.join(output_dir, f"{name}_anomalies.csv")
    pd.DataFrame(anomalies).to_csv(anomalies_path)
    logger.info(f"Anomalies report saved to {anomalies_path}.")


def process_yfinance_data(config):
    """
    Process Yahoo Finance data and validate results in parallel with a progress bar.
    """
    tickers = config["tickers"]["yfinance"]
    start_date = config["date_ranges"]["start_date"]
    end_date = (
        datetime.now().strftime("%Y-%m-%d")
        if config["date_ranges"]["end_date"] == "current"
        else config["date_ranges"]["end_date"]
    )

    def process_ticker(ticker):
        logger.info(f"Processing Yahoo Finance ticker: {ticker}")
        try:
            raw_data = fetch_yfinance_data(ticker, start_date, end_date)
            if raw_data.empty:  # Corrected check for empty DataFrame
                logger.warning(f"No data retrieved for {ticker}. Skipping.")
                return

            # Save the data to Parquet file
            parquet_dir = config["storage"]["Parquet"]["directory"]
            os.makedirs(parquet_dir, exist_ok=True)
            file_path = os.path.join(parquet_dir, f"{ticker}.parquet")
            raw_data.to_parquet(file_path, index=True)
            logger.info(f"Yahoo Finance data for {ticker} saved to {file_path}.")

            # Validate data
            validation_dir = os.path.join(parquet_dir, "validation_reports")
            logger.info(
                f"Starting validation for {ticker}..."
            )  # Start validation logging
            validate_data(raw_data, validation_dir, ticker)
            logger.info(
                f"Validation reports generated for {ticker}."
            )  # End validation logging
        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}", exc_info=True)
        logger.info(f"Finished processing {ticker}.")

    # Use ThreadPoolExecutor for parallel processing of tickers with progress bar
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_ticker, ticker) for ticker in tickers]
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Processing tickers"
        ):  # Added progress bar
            future.result()  # Wait for all tasks to complete
