from pathlib import Path

import pandas as pd


class YahooDataReader:
    def __init__(self, file_path):
        self.file_path = Path(file_path)
        self.data = None

    def load_data(self):
        """Load the Yahoo Finance data."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"Data file not found at {self.file_path}")
        self.data = pd.read_csv(self.file_path, header=[0, 1])  # MultiIndex columns
        self.flatten_columns()
        self.set_date_index()

    def flatten_columns(self):
        """Flatten the hierarchical column names."""
        self.data.columns = ["_".join(col).strip() for col in self.data.columns]
        self.data.rename(columns={"('date', '')": "date"}, inplace=True)

    def set_date_index(self):
        """Set the 'date' column as the index."""
        self.data["date"] = pd.to_datetime(self.data["date"])
        self.data.set_index("date", inplace=True)

    def get_ticker_data(self, ticker):
        """Extract data for a specific ticker."""
        ticker_prefix = f"{ticker}_"
        return self.data.filter(like=ticker_prefix).rename(
            columns=lambda col: col.replace(ticker_prefix, "")
        )

    def get_available_tickers(self):
        """List all tickers available in the data."""
        tickers = set(col.split("_")[0] for col in self.data.columns if "_" in col)
        return tickers
