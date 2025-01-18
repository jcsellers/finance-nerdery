import json
import os
from datetime import datetime

from data_pipleline import (
    get_nyse_calendar,
    ingest_raw_data,
    normalize_data,
    save_to_parquet,
)
from fetchers.fred_fetcher import FredFetcher
from fetchers.yfinance_fetcher import fetch_yfinance_data


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
        print(f"Processing Yahoo Finance ticker: {ticker}")
        raw_data = fetch_yfinance_data(ticker, start_date, end_date)

        if not raw_data:
            print(f"No data retrieved for {ticker} from Yahoo Finance. Skipping.")
            continue

        # Align data with NYSE calendar
        trading_days = get_nyse_calendar(start_date, end_date)

        # Normalize and save data
        cleaned_data = normalize_data(raw_data, trading_days)
        ingest_raw_data(ticker, "yfinance", raw_data, start_date, end_date, config)
        save_to_parquet(cleaned_data, ticker, config)
        print(f"Finished processing {ticker}.\n")


def process_fred_data(config):
    """
    Process all FRED series using FredFetcher and save each series as a separate Parquet file.
    """
    fred_fetcher = FredFetcher(config_path="config/config.json")
    fred_data = fred_fetcher.fetch_data()

    if fred_data.empty:
        print("No FRED data fetched.")
        return

    # Save each FRED series as an individual Parquet file
    parquet_dir = config["storage"]["Parquet"]["directory"]
    os.makedirs(parquet_dir, exist_ok=True)

    for series_id in config["tickers"]["FRED"]:
        # Filter data for the current series ID
        series_data = fred_data[fred_data["fred_ticker"] == series_id]

        if series_data.empty:
            print(f"No data available for {series_id}. Skipping.")
            continue

        # Generate the file path
        file_path = os.path.join(parquet_dir, f"{series_id}.parquet")
        series_data.to_parquet(file_path, index=False)
        print(f"FRED data for {series_id} saved to {file_path}.")


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

    # Process Yahoo Finance tickers
    print("Starting Yahoo Finance data processing...")
    process_yfinance_data(config)
    print("Finished Yahoo Finance data processing.\n")

    # Process FRED series
    print("Starting FRED data processing...")
    process_fred_data(config)
    print("Finished FRED data processing.")


if __name__ == "__main__":
    process_all_data()
import json
import os
from datetime import datetime

from data_pipeline import (
    get_nyse_calendar,
    ingest_raw_data,
    normalize_data,
    save_to_parquet,
)

from fetchers.fred_fetcher import FredFetcher
from fetchers.yfinance_fetcher import fetch_yfinance_data


def validate_data(data, output_dir, name):
    """
    Validate the data and save validation reports.
    """
    print(f"Validating data for {name}...")
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

    # Check for anomalies (e.g., zero or negative values in numeric columns)
    anomalies = {}
    for col in data.select_dtypes(include=["number"]).columns:
        anomalies[col] = {
            "zero_values": (data[col] == 0).sum(),
            "negative_values": (data[col] < 0).sum(),
        }
    anomalies_path = os.path.join(output_dir, f"{name}_anomalies.csv")
    pd.DataFrame(anomalies).to_csv(anomalies_path)
    print(f"Anomalies report saved to {anomalies_path}.")


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
        print(f"Processing Yahoo Finance ticker: {ticker}")
        raw_data = fetch_yfinance_data(ticker, start_date, end_date)

        if not raw_data:
            print(f"No data retrieved for {ticker} from Yahoo Finance. Skipping.")
            continue

        # Align data with NYSE calendar
        trading_days = get_nyse_calendar(start_date, end_date)

        # Normalize and validate data
        cleaned_data = normalize_data(raw_data, trading_days)
        validation_dir = os.path.join(
            config["storage"]["Parquet"]["directory"], "validation_reports"
        )
        validate_data(cleaned_data, validation_dir, ticker)

        # Ingest and save data
        ingest_raw_data(ticker, "yfinance", raw_data, start_date, end_date, config)
        save_to_parquet(cleaned_data, ticker, config)
        print(f"Finished processing {ticker}.\n")


def process_fred_data(config):
    """
    Process all FRED series using FredFetcher and save each series as a separate Parquet file.
    """
    fred_fetcher = FredFetcher(config_path="config/config.json")
    fred_data = fred_fetcher.fetch_data()

    if fred_data.empty:
        print("No FRED data fetched.")
        return

    # Save each FRED series as an individual Parquet file
    parquet_dir = config["storage"]["Parquet"]["directory"]
    os.makedirs(parquet_dir, exist_ok=True)

    for series_id in config["tickers"]["FRED"]:
        # Filter data for the current series ID
        series_data = fred_data[fred_data["fred_ticker"] == series_id]

        if series_data.empty:
            print(f"No data available for {series_id}. Skipping.")
            continue

        # Validate the series data
        validation_dir = os.path.join(parquet_dir, "validation_reports")
        validate_data(series_data, validation_dir, series_id)

        # Save the series data to a Parquet file
        file_path = os.path.join(parquet_dir, f"{series_id}.parquet")
        series_data.to_parquet(file_path, index=False)
        print(f"FRED data for {series_id} saved to {file_path}.")


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

    # Process Yahoo Finance tickers
    print("Starting Yahoo Finance data processing...")
    process_yfinance_data(config)
    print("Finished Yahoo Finance data processing.\n")

    # Process FRED series
    print("Starting FRED data processing...")
    process_fred_data(config)
    print("Finished FRED data processing.")


if __name__ == "__main__":
    process_all_data()
