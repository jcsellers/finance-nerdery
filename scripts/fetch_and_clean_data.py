import logging
import os

import pandas as pd
import yfinance as yf

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define directories for raw and cleaned data
RAW_DIR = "../data/raw"
CLEAN_DIR = "../data/cleaned"


def clean_yfinance_data(file_path, ticker):
    """
    Clean raw data downloaded from Yahoo Finance.
    """
    try:
        df = pd.read_csv(file_path)
        if "Ticker" in df.iloc[0].values:
            df = df.iloc[2:].reset_index(drop=True)
            df.columns = ["Date", "Close", "High", "Low", "Open", "Volume"]

        required_columns = ["Date", "Open", "Close"]
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns in {ticker}: {missing_columns}")

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df.dropna(subset=["Date"], inplace=True)
        df = df[["Date", "Open", "Close"]].sort_values("Date").reset_index(drop=True)
        return df
    except Exception as e:
        logging.error(f"Error cleaning data for {ticker}: {e}")
        return None


def fetch_and_clean(tickers):
    """
    Fetch and clean data for a given list of tickers.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(CLEAN_DIR, exist_ok=True)

    for ticker in tickers:
        logging.info(f"Fetching data for {ticker} using yfinance...")
        try:
            raw_path = os.path.join(RAW_DIR, f"{ticker}.csv")
            clean_path = os.path.join(CLEAN_DIR, f"{ticker}_cleaned.csv")

            df = yf.download(ticker, start="1990-01-02", end="2025-01-03")
            df.to_csv(raw_path)
            logging.info(f"Raw data saved for {ticker} at {raw_path}")

            clean_df = clean_yfinance_data(raw_path, ticker)
            if clean_df is not None:
                clean_df.to_csv(clean_path, index=False)
                logging.info(f"Cleaned data saved to: {clean_path}")
        except Exception as e:
            logging.error(f"Error fetching or cleaning data for {ticker}: {e}")
