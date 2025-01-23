import os

import pandas as pd
import pytest
from aquisition import YahooAcquisition


def test_yahoo_acquisition(tmp_path):
    tickers = ["SPY", "UPRO"]
    start_date = "2020-01-01"
    end_date = "2020-12-31"
    output_dir = tmp_path  # Use a temporary directory for testing

    yahoo = YahooAcquisition(tickers, start_date, end_date, output_dir)
    data = yahoo.fetch_data()
    yahoo.save_data(data)

    # Check if files exist
    csv_path = os.path.join(output_dir, "yahoo_data.csv")
    parquet_path = os.path.join(output_dir, "yahoo_data.parquet")
    assert os.path.exists(csv_path), "CSV file was not created."
    assert os.path.exists(parquet_path), "Parquet file was not created."

    # Validate CSV structure
    df = pd.read_csv(csv_path, index_col="Date", parse_dates=True)
    assert all(ticker in df.columns for ticker in tickers), "Missing tickers in CSV."
