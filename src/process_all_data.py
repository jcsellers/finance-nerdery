import json
import logging
import os
from datetime import datetime

import pandas as pd

from data_pipleline import validate_data  # Import validate_data here
from data_pipleline import (
    get_nyse_calendar,
    ingest_raw_data,
    normalize_data,
    save_to_parquet,
)
from fetchers.fred_fetcher import FredFetcher
from fetchers.yfinance_fetcher import fetch_yfinance_data

# Configure logging
logging.basicConfig(
    filename="./logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("DataPipeline")


def process_yfinance_data(config):
    """
    Process all Yahoo Finance tickers and save their data.
    """
    tickers = config["tickers"]["yfinance"]
    start_date = config["date_ranges"]["start_date"]
    end_date = (
        datetime.now().strftime("%Y-%m-%d")
        if config["date_ranges"]["end_date"] == "current"
        else config["date_ranges"]["end_date"]
    )

    for ticker in tickers:
        logger.info(f"Processing Yahoo Finance ticker: {ticker}")
        try:
            raw_data = fetch_yfinance_data(ticker, start_date, end_date)

            if not raw_data:
                logger.warning(
                    f"No data retrieved for {ticker} from Yahoo Finance. Skipping."
                )
                continue

            # Convert raw_data back to DataFrame
            raw_data_df = pd.DataFrame.from_dict(raw_data, orient="index")

            # Align data with NYSE calendar
            trading_days = get_nyse_calendar(start_date, end_date)

            # Normalize and save data
            cleaned_data = normalize_data(raw_data_df, trading_days)
            ingest_raw_data(ticker, "yfinance", raw_data, start_date, end_date, config)
            save_to_parquet(cleaned_data, ticker, config)
            logger.info(f"Finished processing {ticker}.")
        except Exception as e:
            logger.error(
                f"Error processing Yahoo Finance data for {ticker}: {e}", exc_info=True
            )


def process_fred_data(config):
    """
    Process all FRED series using FredFetcher and save each series as a separate Parquet file.
    """
    fred_fetcher = FredFetcher(config_path="config/config.json")
    fred_data = fred_fetcher.fetch_data()

    if fred_data.empty:
        logger.warning("No FRED data fetched.")
        return

    # Save each FRED series as an individual Parquet file
    parquet_dir = config["storage"]["Parquet"]["directory"]
    os.makedirs(parquet_dir, exist_ok=True)

    for series_id in config["tickers"]["FRED"]:
        try:
            # Filter data for the current series ID
            logger.info(f"Processing FRED series: {series_id}")
            series_data = fred_data[fred_data["fred_ticker"] == series_id]

            if series_data.empty:
                logger.warning(f"No data available for {series_id}. Skipping.")
                continue

            # Validate the series data
            validation_dir = os.path.join(parquet_dir, "validation_reports")
            validate_data(series_data, validation_dir, series_id)

            # Save the series data to a Parquet file
            file_path = os.path.join(parquet_dir, f"{series_id}.parquet")
            series_data.to_parquet(file_path, index=False)
            logger.info(f"FRED data for {series_id} saved to {file_path}.")
        except Exception as e:
            logger.error(
                f"Error processing FRED series {series_id}: {e}", exc_info=True
            )


def process_all_data():
    """
    Process all data for both Yahoo Finance and FRED tickers.
    """
    # Resolve the path to config.json
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Directory of this script
    config_path = os.path.join(
        script_dir, "../config/config.json"
    )  # Adjust the path based on your structure

    # Load configuration
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        logger.info("Configuration loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}", exc_info=True)
        return

    # Process Yahoo Finance tickers
    logger.info("Starting Yahoo Finance data processing...")
    try:
        process_yfinance_data(config)
        logger.info("Finished Yahoo Finance data processing.")
    except Exception as e:
        logger.error(f"Error in Yahoo Finance data processing: {e}", exc_info=True)

    # Process FRED series
    logger.info("Starting FRED data processing...")
    try:
        process_fred_data(config)
        logger.info("Finished FRED data processing.")
    except Exception as e:
        logger.error(f"Error in FRED data processing: {e}", exc_info=True)


if __name__ == "__main__":
    process_all_data()
