import logging
import os

import pandas as pd
from align_data import align_datasets
from database_utils import ingest_file, validate_dataframe
from fetch_and_clean_data import fetch_and_clean
from fetch_fred_data import fetch_fred_data

# Base directory
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Paths
TICKER_PATH = os.path.join(BASE_DIR, "../data/ticker_file.csv")
RAW_DIR = os.path.join(BASE_DIR, "../data/raw")
CLEAN_DIR = os.path.join(BASE_DIR, "../data/cleaned")
ALIGNED_DIR = os.path.join(BASE_DIR, "../data/aligned")
DB_PATH = os.path.join(BASE_DIR, "../data/output/aligned_data.db")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def load_tickers(filepath=TICKER_PATH):
    logging.info(f"Looking for ticker file at: {os.path.abspath(filepath)}")

    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"Ticker file not found at: {os.path.abspath(filepath)}"
        )

    tickers_df = pd.read_csv(filepath)
    if "Type" not in tickers_df.columns or "Symbol" not in tickers_df.columns:
        raise ValueError("Ticker file must contain 'Type' and 'Symbol' columns.")

    yahoo_tickers = tickers_df[tickers_df["Type"] == "Ticker"]["Symbol"].tolist()
    fred_tickers = tickers_df[tickers_df["Type"] == "FRED"]["Symbol"].tolist()

    return yahoo_tickers, fred_tickers


def fetch_and_save_raw_data(yahoo_tickers, fred_tickers):
    logging.info("Fetching raw data...")
    fetch_and_clean(tickers=yahoo_tickers)
    fetch_fred_data(fred_tickers, output_dir=RAW_DIR)
    logging.info("Raw data fetched and saved.")


def align_and_save():
    logging.info("Aligning raw data...")
    align_datasets(input_dir=RAW_DIR, output_dir=ALIGNED_DIR)

    aligned_files = [f for f in os.listdir(ALIGNED_DIR) if f.endswith("_aligned.csv")]
    logging.info(f"Aligned files: {aligned_files}")


def ingest_aligned_data():
    logging.info(f"Ingesting aligned data into database: {DB_PATH}")
    aligned_files = [f for f in os.listdir(ALIGNED_DIR) if f.endswith("_aligned.csv")]

    for file in aligned_files:
        filepath = os.path.join(ALIGNED_DIR, file)
        try:
            logging.info(f"Ingesting file: {filepath}")
            dataframe = pd.read_csv(filepath)
            validate_dataframe(dataframe)
            ingest_file(filepath, DB_PATH)
            logging.info(f"Ingested file: {filepath}")
        except Exception as e:
            logging.error(f"Error ingesting {file}: {e}")


def main():
    logging.info("Starting pipeline...")

    yahoo_tickers, fred_tickers = load_tickers()

    fetch_and_save_raw_data(yahoo_tickers, fred_tickers)
    align_and_save()
    ingest_aligned_data()

    logging.info("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
