import os
from fredapi import Fred
import pandas as pd
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

FRED_API_KEY = os.getenv("FRED_API_KEY")

if not FRED_API_KEY:
    raise ValueError("FRED_API_KEY is not set. Please check your .env file.")

def fetch_fred_data(fred_symbols, output_dir="../data/economic"):
    """
    Fetch economic data from FRED and save to the specified directory.

    Parameters:
        fred_symbols (list): List of FRED series symbols to fetch.
        output_dir (str): Directory to save the fetched data.
    """
    os.makedirs(output_dir, exist_ok=True)

    fred = Fred(api_key=FRED_API_KEY)

    for symbol in fred_symbols:
        try:
            logging.info(f"Fetching FRED series: {symbol}")
            data = fred.get_series(symbol)
            df = pd.DataFrame(data, columns=["Value"])
            df.index.name = "Date"

            # Map FRED data to the expected schema
            df.reset_index(inplace=True)
            df["Open"] = df["Value"]
            df["Close"] = df["Value"]
            df["High"] = df["Value"]
            df["Low"] = df["Value"]
            df["Volume"] = 0  # Default value for Volume
            df = df[["Date", "Open", "Close", "High", "Low", "Volume"]]  # Reorder columns

            filepath = os.path.join(output_dir, f"{symbol}.csv")
            df.to_csv(filepath, index=False)
            logging.info(f"Saved FRED series: {symbol} to {filepath}")
        except Exception as e:
            logging.error(f"Error fetching FRED series {symbol}: {e}")

if __name__ == "__main__":
    # Example usage
    fetch_fred_data(["GDP", "UNRATE", "CPI"])
