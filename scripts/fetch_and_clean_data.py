import os
import pandas as pd
import yfinance as yf

# Define ticker list
tickers = ["UPRO", "SSO", "SPY", "TLT", "KMLM", "GLD", "^VIX", "^GSPC"]

# Define folders
raw_folder = "raw_ticker_data"
cleaned_folder = "cleaned_data"

# Create directories if they don't exist
os.makedirs(raw_folder, exist_ok=True)
os.makedirs(cleaned_folder, exist_ok=True)

def fetch_data(ticker):
    """
    Fetch raw data for a given ticker and save to the raw folder.
    """
    try:
        print(f"Fetching data for {ticker}...")
        data = yf.download(ticker, start="1990-01-01", progress=False)
        
        # Ensure Date is part of the data
        data.reset_index(inplace=True)
        
        # Save the raw data
        raw_file_path = os.path.join(raw_folder, f"{ticker}.csv")
        data.to_csv(raw_file_path, index=False)
        print(f"Raw data saved for {ticker} at {raw_file_path}")
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")

def validate_cleaned_data(df):
    """
    Validate the cleaned data by checking for missing values, column names, and date formatting.
    """
    # Ensure no missing values in critical columns
    required_columns = ["Date", "Close", "High", "Low", "Open", "Volume"]
    for column in required_columns:
        if column not in df.columns:
            raise ValueError(f"Missing column: {column}")
        if df[column].isnull().any():
            raise ValueError(f"Missing values detected in column: {column}")
    
    # Ensure date format is consistent
    try:
        pd.to_datetime(df["Date"], format="%Y-%m-%d")
    except Exception as e:
        raise ValueError(f"Invalid date format in 'Date' column: {e}")

    print("Validation passed.")
    return True

def clean_data(ticker):
    """
    Clean raw data for a given ticker and save to the cleaned folder after validation.
    """
    try:
        print(f"Cleaning data for {ticker}...")
        raw_file_path = os.path.join(raw_folder, f"{ticker}.csv")
        if not os.path.exists(raw_file_path):
            print(f"Raw file not found for {ticker}, skipping...")
            return

        # Read raw data
        data = pd.read_csv(raw_file_path)

        # Ensure the 'Date' column exists and handle missing values
        if "Date" not in data.columns:
            data.reset_index(inplace=True)  # Convert index to a column if 'Date' is the index
        if data["Date"].isnull().any():
            print(f"Missing dates found in data for {ticker}. Filling or dropping missing rows...")
            data.dropna(subset=["Date"], inplace=True)  # Drop rows with missing Date values

        # Select only required columns and rename
        required_columns = ["Date", "Close", "High", "Low", "Open", "Volume"]
        available_columns = [col for col in required_columns if col in data.columns]
        if len(available_columns) < len(required_columns):
            raise ValueError(f"Missing columns in data for {ticker}: {set(required_columns) - set(available_columns)}")

        cleaned_data = data[available_columns].copy()

        # Ensure Date is in the correct format
        cleaned_data["Date"] = pd.to_datetime(cleaned_data["Date"]).dt.strftime("%Y-%m-%d")

        # Validate the cleaned data
        validate_cleaned_data(cleaned_data)

        # Save cleaned data
        cleaned_file_path = os.path.join(cleaned_folder, f"{ticker}_cleaned.csv")
        cleaned_data.to_csv(cleaned_file_path, index=False)
        print(f"Validated and updated cleaned file for {ticker} at {cleaned_file_path}")
    except Exception as e:
        print(f"Error cleaning data for {ticker}: {e}")

# Main workflow
for ticker in tickers:
    fetch_data(ticker)
    clean_data(ticker)

print("Fetching, cleaning, and validation process completed.")
