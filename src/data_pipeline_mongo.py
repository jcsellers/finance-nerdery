import json
import os
from datetime import datetime

import pandas as pd
from pandas_market_calendars import get_calendar
from pymongo import MongoClient

from fetchers.fred_fetcher import fetch_fred_data
from fetchers.yfinance_fetcher import fetch_yfinance_data


def load_config():
    """
    Load configuration from projectroot/config/config.json.
    """
    config_path = os.path.join(os.path.dirname(__file__), "../config/config.json")
    with open(config_path, "r") as f:
        return json.load(f)


def get_nyse_calendar(start_date, end_date):
    """
    Retrieve NYSE trading days within the specified date range.
    """
    nyse = get_calendar("NYSE")  # Returns a calendar object
    schedule = nyse.schedule(
        start_date=start_date, end_date=end_date
    )  # Generates the schedule DataFrame
    return schedule.index  # Return the trading days as DatetimeIndex


def ingest_raw_data(ticker, source, data, start_date, end_date, config):
    """
    Ingest raw data into MongoDB using upsert to avoid duplicates and allow updates.
    """
    client = MongoClient(config["storage"]["MongoDB"]["connection_string"])
    db = client[config["storage"]["MongoDB"]["database"]]
    collection = db[config["storage"]["MongoDB"]["collections"]["raw_data"]]

    # Create or update document
    document = {
        "ticker": ticker,
        "source": source,
        "data": data,
        "metadata": {
            "retrieval_timestamp": datetime.utcnow(),
            "schema_version": "1.0",
            "retry_count": 0,
        },
        "date_range": {"start_date": start_date, "end_date": end_date},
    }

    # Use upsert to avoid duplicates
    result = collection.update_one(
        {
            "ticker": ticker,
            "source": source,
            "date_range.start_date": start_date,
            "date_range.end_date": end_date,
        },
        {"$set": document},
        upsert=True,
    )

    if result.upserted_id:
        print(f"Inserted document ID: {result.upserted_id}")
    else:
        print(f"Updated existing document for {ticker} from {source}.")


def normalize_data(raw_data, trading_days):
    """
    Normalize raw data to ensure consistent fields and align with trading days.
    """
    # Convert raw data to a DataFrame
    df = pd.DataFrame.from_dict(raw_data, orient="index")

    # Align with NYSE trading days
    df.index = pd.to_datetime(df.index)
    df = df.reindex(trading_days)

    # Fill missing fields
    if "volume" not in df.columns:
        df["volume"] = None
    if "open" not in df.columns:
        df["open"] = df["close"]  # Example: Use 'close' price if 'open' is missing

    # Reset index
    df = df.sort_index()
    return df


def save_to_parquet(cleaned_data, ticker, config):
    """
    Save normalized data to a Parquet file.
    """
    parquet_dir = config["storage"]["Parquet"]["directory"]
    os.makedirs(parquet_dir, exist_ok=True)

    filepath = os.path.join(parquet_dir, f"{ticker}.parquet")
    cleaned_data.to_parquet(filepath)
    print(f"Saved cleaned data for {ticker} to {filepath}.")


def pipeline(ticker, source, start_date, end_date):
    """
    Orchestrate the data pipeline: retrieval, ingestion, normalization, and saving.
    """
    # Load configuration
    config = load_config()

    # Retrieve data from the appropriate source
    if source == "yfinance":
        raw_data = fetch_yfinance_data(ticker, start_date, end_date)
    elif source == "FRED":
        raw_data = fetch_fred_data(
            ticker, start_date, end_date, config["FRED"]["api_key"]
        )
    else:
        raise ValueError(f"Unsupported data source: {source}")

    if not raw_data:
        print(f"No data retrieved for {ticker} from {source}. Skipping.")
        return

    # Get NYSE trading days
    trading_days = get_nyse_calendar(start_date, end_date)

    # Ingest raw data
    ingest_raw_data(ticker, source, raw_data, start_date, end_date, config)

    # Normalize raw data
    cleaned_data = normalize_data(raw_data, trading_days)

    # Save to Parquet
    save_to_parquet(cleaned_data, ticker, config)


if __name__ == "__main__":
    # Define parameters
    test_ticker = "UPRO"
    test_source = "yfinance"
    test_start_date = "2020-01-02"
    test_end_date = "2025-01-02"

    # Run the pipeline
    pipeline(
        ticker=test_ticker,
        source=test_source,
        start_date=test_start_date,
        end_date=test_end_date,
    )
