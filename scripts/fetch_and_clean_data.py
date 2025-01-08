import os
import pandas as pd
import yfinance as yf
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

RAW_DIR = "../data/raw"
CLEAN_DIR = "../data/cleaned"
TICKERS = ["UPRO", "SSO", "SPY", "TLT", "GLD"]


def clean_yfinance_data(file_path, ticker):
    """
    Clean raw data downloaded from Yahoo Finance.

    Parameters:
        file_path (str): Path to the raw CSV file.
        ticker (str): The ticker symbol for the data.

    Returns:
        DataFrame: Cleaned DataFrame or None if an error occurs.
    """
    try:
        # Attempt to read the CSV
        df = pd.read_csv(file_path)

        # Remove metadata rows
        if "Ticker" in df.iloc[0].values:
            df = df.iloc[2:].reset_index(drop=True)
            df.columns = ["Date", "Close", "High", "Low", "Open", "Volume"]

        # Validate required columns
        required_columns = ["Date", "Open", "Close"]
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns in {ticker}: {missing_columns}")

        # Convert Date column to datetime and drop invalid rows
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df.dropna(subset=["Date"], inplace=True)

        # Filter relevant columns and sort
        df = df[["Date", "Open", "Close"]].sort_values("Date").reset_index(drop=True)
        return df
    except Exception as e:
        logging.error(f"Error cleaning data for {ticker}: {e}")
        return None


def fetch_and_clean_yfinance_data(ticker):
    """
    Fetch and clean data for a given ticker.

    Parameters:
        ticker (str): The ticker symbol to fetch and clean.
    """
    raw_path = os.path.join(RAW_DIR, f"{ticker}.csv")
    clean_path = os.path.join(CLEAN_DIR, f"{ticker}_cleaned.csv")
    try:
        os.makedirs(RAW_DIR, exist_ok=True)
        os.makedirs(CLEAN_DIR, exist_ok=True)

        # Fetch data and save raw
        df = yf.download(ticker, start="1990-01-02", end="2025-01-03")
        df.to_csv(raw_path)
        logging.info(f"Raw data saved for {ticker} at {raw_path}")

        # Clean the data
        clean_df = clean_yfinance_data(raw_path, ticker)
        if clean_df is not None:
            clean_df.to_csv(clean_path, index=False)
            logging.info(f"Cleaned data saved to: {clean_path}")
    except Exception as e:
        logging.error(f"Error fetching or cleaning data for {ticker}: {e}")


def main():
    """
    Main function to fetch and clean data for all tickers.
    """
    for ticker in TICKERS:
        logging.info(f"Fetching data for {ticker} using yfinance...")
        fetch_and_clean_yfinance_data(ticker)


if __name__ == "__main__":
    main()

