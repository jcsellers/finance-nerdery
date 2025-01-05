
import os
import pandas as pd
import yfinance as yf
import logging
from argparse import ArgumentParser

# Logging configuration
logging.basicConfig(filename="error_log.txt", level=logging.ERROR,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Parse arguments
parser = ArgumentParser()
parser.add_argument("--start-date", type=str, default="1990-01-01", help="Start date for fetching data")
parser.add_argument("--tickers-file", type=str, default="tickers.txt", help="File containing list of tickers")
parser.add_argument("--test-mode", action="store_true", help="Run script in test mode with limited tickers")
args = parser.parse_args()

START_DATE = args.start_date
TICKERS_FILE = args.tickers_file
TEST_MODE = args.test_mode

# Read tickers from file
if os.path.exists(TICKERS_FILE):
    with open(TICKERS_FILE, 'r') as f:
        tickers = [line.strip() for line in f if line.strip()]
else:
    logging.error(f"Tickers file {TICKERS_FILE} not found.")
    raise FileNotFoundError(f"Tickers file {TICKERS_FILE} not found.")

if TEST_MODE:
    tickers = tickers[:2]

# Get the project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Define folders
raw_folder = os.path.join(project_root, "data", "raw")
cleaned_folder = os.path.join(project_root, "data", "cleaned")

# Create directories if they don't exist
os.makedirs(raw_folder, exist_ok=True)
os.makedirs(cleaned_folder, exist_ok=True)

def fetch_data(ticker):
    """
    Fetch raw data for a given ticker and save to the raw folder.
    """
    try:
        print(f"Fetching data for {ticker}...")
        data = yf.download(ticker, start=START_DATE, progress=False)
        
        # Ensure Date is part of the data
        data.reset_index(inplace=True)
        
        # Save the raw data
        raw_file_path = os.path.join(raw_folder, f"{ticker}.csv")
        data.to_csv(raw_file_path, index=False)
        print(f"Raw data saved for {ticker} at {raw_file_path}")
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")

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
            print(f"Missing dates found in data for {ticker}. Dropping missing rows...")
            data.dropna(subset=["Date"], inplace=True)  # Drop rows with missing Date values

        # Select only required columns and rename
        required_columns = ["Date", "Close", "High", "Low", "Open", "Volume"]
        available_columns = [col for col in required_columns if col in data.columns]
        if len(available_columns) < len(required_columns):
            raise ValueError(f"Missing columns in data for {ticker}: {set(required_columns) - set(available_columns)}")

        cleaned_data = data[available_columns].copy()

        # Ensure Date is in the correct format
        cleaned_data["Date"] = pd.to_datetime(cleaned_data["Date"]).dt.strftime("%Y-%m-%d")

        # Remove duplicates
        cleaned_data = cleaned_data.drop_duplicates(subset="Date")

        # Validate the cleaned data
        validate_cleaned_data(cleaned_data)

        # Save cleaned data
        cleaned_file_path = os.path.join(cleaned_folder, f"{ticker}_cleaned.csv")
        cleaned_data.to_csv(cleaned_file_path, index=False)
        print(f"Validated and updated cleaned file for {ticker} at {cleaned_file_path}")
    except Exception as e:
        logging.error(f"Error cleaning data for {ticker}: {e}")

def validate_output_folders():
    """
    Validate that the raw and cleaned data folders contain expected files.
    Log the row counts for each file.
    """
    print("\nValidating output folders...")

    # Validate raw folder
    print("\nRaw Files:")
    for file in os.listdir(raw_folder):
        file_path = os.path.join(raw_folder, file)
        if file.endswith(".csv"):
            rows = len(pd.read_csv(file_path))
            print(f" - {file}: {rows} rows")
        else:
            print(f" - {file}: Not a CSV file")

    # Validate cleaned folder
    print("\nCleaned Files:")
    for file in os.listdir(cleaned_folder):
        file_path = os.path.join(cleaned_folder, file)
        if file.endswith(".csv"):
            rows = len(pd.read_csv(file_path))
            print(f" - {file}: {rows} rows")
        else:
            print(f" - {file}: Not a CSV file")

def check_data_alignment():
    """
    Ensure all datasets in the cleaned folder align by their start and end dates.
    """
    print("\nChecking data alignment...")
    start_dates = []
    end_dates = []

    for file in os.listdir(cleaned_folder):
        file_path = os.path.join(cleaned_folder, file)
        if file.endswith(".csv"):
            df = pd.read_csv(file_path)
            start_dates.append(pd.to_datetime(df["Date"].min()))
            end_dates.append(pd.to_datetime(df["Date"].max()))

    # Ensure alignment
    if len(start_dates) > 0 and len(end_dates) > 0:
        aligned_start = max(start_dates)
        aligned_end = min(end_dates)
        print(f" - Aligned Start Date: {aligned_start}")
        print(f" - Aligned End Date: {aligned_end}")

        if aligned_start > aligned_end:
            print(" - Warning: Datasets do not align properly!")
        else:
            print(" - All datasets are aligned correctly.")

# Main workflow
for ticker in tickers:
    fetch_data(ticker)
    clean_data(ticker)

validate_output_folders()
check_data_alignment()

print("Fetching, cleaning, and validation process completed.")
