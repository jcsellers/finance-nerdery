import logging
import os

import pandas as pd
import synthetic_dataset_generator
from align_data import align_datasets
from create_sqlite_db import create_and_populate_unified_table
from fetch_and_clean_data import fetch_and_clean
from fetch_fred_data import fetch_fred_data

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
TICKER_FILE = os.path.join(os.path.dirname(__file__), "../data/ticker_file.csv")
DB_PATH = os.path.join(os.path.dirname(__file__), "../data/output/aligned_data.db")


def load_symbols(file_path=TICKER_FILE):
    try:
        df = pd.read_csv(file_path)
        fred_symbols = df[df["Type"] == "FRED"]["Symbol"].tolist()
        yahoo_symbols = df[df["Type"] == "Ticker"]["Symbol"].tolist()
        return fred_symbols, yahoo_symbols
    except Exception as e:
        logging.error(f"Error loading symbols: {e}")
        return [], []


def main():
    logging.info("Starting pipeline...")
    fred_symbols, yahoo_symbols = load_symbols()

    fetch_fred_data(fred_symbols)
    synthetic_dataset_generator.main()
    fetch_and_clean(yahoo_symbols)
    align_datasets()

    datasets = {
        ticker: f"../data/aligned/{ticker}_cleaned.csv" for ticker in yahoo_symbols
    }
    datasets.update(
        {symbol: f"../data/aligned/{symbol}.csv" for symbol in fred_symbols}
    )
    datasets.update(
        {
            "SYN_LINEAR": "../data/aligned/SYN_LINEAR_cleaned.csv",
            "SYN_CASH": "../data/aligned/SYN_CASH_cleaned.csv",
        }
    )

    create_and_populate_unified_table(DB_PATH, datasets)
    logging.info("Pipeline completed successfully.")


if __name__ == "__main__":
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    main()
