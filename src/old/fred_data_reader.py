from pathlib import Path


class FredDataReader:
    def __init__(self, file_path):
        self.file_path = Path(file_path)
        self.data = None

    def load_data(self):
        """Load the FRED data."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"Data file not found at {self.file_path}")
        self.data = pd.read_csv(self.file_path, parse_dates=["date"])

    def get_ticker_data(self, ticker, filter_actual=True):
        """Extract data for a specific ticker."""
        if self.data is None:
            raise ValueError("Data not loaded. Call `load_data()` first.")

        # Filter by ticker
        filtered_data = self.data[self.data["ticker"] == ticker]

        # Optionally filter by data_flag
        if filter_actual:
            filtered_data = filtered_data[filtered_data["data_flag"] == "actual"]

        # Return a DataFrame indexed by date
        return (
            filtered_data[["date", "value"]]
            .set_index("date")
            .rename(columns={"value": ticker})
        )

    def get_available_tickers(self):
        """List all tickers available in the data."""
        if self.data is None:
            raise ValueError("Data not loaded. Call `load_data()` first.")
        return self.data["ticker"].unique()
