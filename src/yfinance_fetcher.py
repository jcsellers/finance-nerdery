import pandas as pd
import yfinance as yf


class YahooFinanceFetcher:
    def __init__(self, missing_data_handling="interpolate"):
        """
        Initialize the YahooFinanceFetcher.

        :param missing_data_handling: Strategy for handling missing data ('interpolate', 'forward_fill', or 'flag').
        """
        self.missing_data_handling = missing_data_handling

    def fetch_data(self, ticker, start_date, end_date):
        """
        Fetch data from Yahoo Finance and transform into OHLCV format.

        :param ticker: Yahoo Finance ticker symbol.
        :param start_date: Start date for the data fetch.
        :param end_date: End date for the data fetch.
        :return: DataFrame in OHLCV format.
        """
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if df.empty:
                raise ValueError(f"No data returned for ticker {ticker}.")
            df = df[["Open", "High", "Low", "Close", "Volume"]]
        except Exception as e:
            raise RuntimeError(f"Error fetching data for {ticker}: {e}")

        # Handle missing data
        if self.missing_data_handling == "interpolate":
            df = df.interpolate(method="linear")
        elif self.missing_data_handling == "forward_fill":
            df = df.ffill()
        elif self.missing_data_handling == "flag":
            pass  # Retain missing values
        else:
            raise ValueError(
                f"Unsupported missing_data_handling: {self.missing_data_handling}"
            )

        return df
