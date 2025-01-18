import json
import os
from datetime import datetime

import pandas as pd
from pandas_market_calendars import get_calendar
from pymongo import MongoClient

from fetchers.fred_fetcher import FredFetcher
from fetchers.yfinance_fetcher import fetch_yfinance_data


def get_nyse_calendar(start_date, end_date):
    """
    Retrieve NYSE trading days within the specified date range.
    """
    nyse = get_calendar("NYSE")
    schedule = nyse.schedule(start_date=start_date, end_date=end_date)
    return schedule.index


def ingest_raw_data(ticker, source, data, start_date, end_date, config):
    """
    Ingest raw data into MongoDB.
    """
    client = MongoClient(config["storage"]["MongoDB"]["connection_string"])
    db = client[config["storage"]["MongoDB"]["database"]]
    collection = db[config["storage"]["MongoDB"]["collections"]["raw_data"]]

    # Convert DataFrame to dictionary and ensure keys are strings
    if isinstance(data, pd.DataFrame):
        data = data.to_dict(orient="index")
        data = {
            str(key): value for key, value in data.items()
        }  # Convert keys to strings

    document = {
        "ticker": ticker,
        "source": source,
        "data": data,  # Now with string keys
        "metadata": {
            "retrieval_timestamp": datetime.utcnow(),
            "schema_version": "1.0",
            "retry_count": 0,
        },
        "date_range": {"start_date": start_date, "end_date": end_date},
    }
    collection.insert_one(document)
    print(f"Ingested raw data for {ticker} from {source}.")


def normalize_data(raw_data, trading_days):
    """
    Normalize raw data to ensure consistent fields and align with trading days.
    """
    # Ensure the input is a DataFrame
    if not isinstance(raw_data, pd.DataFrame):
        raise ValueError("normalize_data expects a Pandas DataFrame.")

    # Align with NYSE trading days
    raw_data.index = pd.to_datetime(raw_data.index)
    raw_data = raw_data.reindex(trading_days)

    # Fill missing fields
    if "volume" not in raw_data.columns:
        raw_data["volume"] = None
    if "open" not in raw_data.columns:
        raw_data["open"] = raw_data[
            "close"
        ]  # Example: Use 'close' price if 'open' is missing

    return raw_data.sort_index()


def validate_data(data, output_dir, name):
    """
    Validate the data and save reports.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Generate summary statistics
    summary = data.describe(include="all")
    summary_path = os.path.join(output_dir, f"{name}_summary.csv")
    summary.to_csv(summary_path)
    print(f"Summary statistics saved to {summary_path}.")

    # Check for missing values
    missing = data.isnull().sum()
    missing_path = os.path.join(output_dir, f"{name}_missing.csv")
    missing.to_csv(missing_path, header=["missing_count"])
    print(f"Missing values report saved to {missing_path}.")

    # Check for anomalies
    anomalies = {}
    for col in data.select_dtypes(include=["number"]).columns:
        anomalies[col] = {
            "zero_values": (data[col] == 0).sum(),
            "negative_values": (data[col] < 0).sum(),
        }
    anomalies_path = os.path.join(output_dir, f"{name}_anomalies.csv")
    pd.DataFrame(anomalies).to_csv(anomalies_path)
    print(f"Anomalies report saved to {anomalies_path}.")


def save_to_parquet(cleaned_data, ticker, config):
    """
    Save normalized data to a Parquet file.
    """
    parquet_dir = config["storage"]["Parquet"]["directory"]
    os.makedirs(parquet_dir, exist_ok=True)

    filepath = os.path.join(parquet_dir, f"{ticker}.parquet")
    cleaned_data.to_parquet(filepath)
    print(f"Saved cleaned data for {ticker} to {filepath}.")


def process_data(ticker, source, start_date, end_date, config):
    """
    Process a single ticker or series: fetch, normalize, validate, and save.
    """
    print(f"Processing {ticker} from {source}...")
    if source == "yfinance":
        raw_data = fetch_yfinance_data(ticker, start_date, end_date)
    elif source == "FRED":
        fred_fetcher = FredFetcher(config_path="config/config.json")
        fred_data = fred_fetcher.fetch_data()

        # Ensure fred_data is not empty
        if fred_data.empty:
            print(f"No FRED data available for {ticker}. Skipping.")
            return

        raw_data = fred_data[fred_data["fred_ticker"] == ticker]
    else:
        raise ValueError(f"Unsupported source: {source}")

    # Check if raw_data is valid
    if raw_data is None or raw_data.empty:
        print(f"No data retrieved for {ticker} from {source}. Skipping.")
        return

    # Align data with trading days
    trading_days = get_nyse_calendar(start_date, end_date)
    normalized_data = normalize_data(raw_data, trading_days)

    # Ingest raw data into MongoDB
    ingest_raw_data(ticker, source, raw_data, start_date, end_date, config)

    # Validate the normalized data
    validation_dir = os.path.join(
        config["storage"]["Parquet"]["directory"], "validation_reports"
    )
    validate_data(normalized_data, validation_dir, ticker)

    # Save the normalized data to Parquet
    save_to_parquet(normalized_data, ticker, config)


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
    with open(config_path, "r") as f:
        config = json.load(f)

    # Resolve dates
    start_date = config["date_ranges"]["start_date"]
    end_date = (
        datetime.now().strftime("%Y-%m-%d")
        if config["date_ranges"]["end_date"] == "current"
        else config["date_ranges"]["end_date"]
    )

    # Process Yahoo Finance tickers
    print("Starting Yahoo Finance data processing...")
    for ticker in config["tickers"]["yfinance"]:
        process_data(ticker, "yfinance", start_date, end_date, config)
    print("Finished Yahoo Finance data processing.\n")

    # Process FRED series
    print("Starting FRED data processing...")
    for series_id in config["tickers"]["FRED"]:
        process_data(series_id, "FRED", start_date, end_date, config)
    print("Finished FRED data processing.")


if __name__ == "__main__":
    process_all_data()
