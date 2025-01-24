import logging
import os

import pandas as pd
import vectorbt as vbt
from tenacity import retry, stop_after_attempt, wait_exponential


class YahooAcquisition:
    def __init__(self, tickers, start_date, end_date, output_dir):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.output_dir = output_dir

    def fetch_data(self):
        """Fetch Yahoo Finance data using vectorbt."""
        logging.info(f"Fetching Yahoo Finance data for tickers: {self.tickers}")
        valid_tickers = []
        dataframes = []

        for ticker in self.tickers:
            try:
                ticker_data = vbt.YFData.download(
                    ticker.strip().upper(),
                    start=self.start_date,
                    end=self.end_date,
                ).get("Close")
                if not ticker_data.empty:
                    dataframes.append(ticker_data)
                    valid_tickers.append(ticker)
                else:
                    logging.warning(f"No data found for ticker: {ticker}")
            except Exception as e:
                logging.warning(f"Failed to fetch data for ticker {ticker}: {e}")

        if not dataframes:
            raise RuntimeError("No valid data fetched for any ticker.")

        # Combine all ticker data into a single DataFrame
        data = pd.concat(dataframes, axis=1)
        data.columns = valid_tickers  # Use valid tickers as column names
        data = data.ffill().bfill()  # Fill missing data
        logging.info(f"Fetched data with valid columns: {data.columns}")
        return data

    def save_data(self, data):
        """Save Yahoo Finance data to CSV and Parquet formats."""
        csv_path = f"{self.output_dir}/yahoo_data.csv"
        parquet_path = f"{self.output_dir}/yahoo_data.parquet"

        data.to_csv(csv_path, index=True)
        logging.info(f"Saved Yahoo Finance data to {csv_path}")

        data.to_parquet(parquet_path)
        logging.info(f"Saved Yahoo Finance data to {parquet_path}")


class FredAcquisition:
    def __init__(self, api_key, cache_dir, missing_data_handling="interpolate"):
        self.api_key = api_key
        self.cache_dir = cache_dir
        self.missing_data_handling = missing_data_handling
        from fredapi import Fred

        self.fred = Fred(api_key=self.api_key)

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_series(self, series_id, start_date, end_date):
        """Fetch a FRED series and cache the result."""
        logging.info(
            f"Fetching FRED series {series_id} from {start_date} to {end_date}"
        )
        sanitized_start_date = pd.Timestamp(start_date).strftime("%Y-%m-%d")
        sanitized_end_date = pd.Timestamp(end_date).strftime("%Y-%m-%d")
        cache_path = os.path.join(
            self.cache_dir,
            f"{series_id}_{sanitized_start_date}_{sanitized_end_date}.csv",
        )

        # Use cached data if available
        if os.path.exists(cache_path):
            logging.info(f"Loading cached data for series {series_id}")
            return pd.read_csv(cache_path, index_col="Date", parse_dates=True)

        # Fetch from API
        try:
            series = self.fred.get_series(
                series_id, observation_start=start_date, observation_end=end_date
            )
            if series.empty:
                logging.warning(f"No data found for series {series_id}")
                return pd.DataFrame(columns=["Date", "Value"]).set_index("Date")

            # Format and save data
            df = series.reset_index()
            df.columns = ["Date", "Value"]
            df.set_index("Date", inplace=True)
            df.to_csv(cache_path)
            logging.info(f"Saved FRED series {series_id} to cache: {cache_path}")
            return df
        except Exception as e:
            logging.error(f"Failed to fetch series {series_id}: {e}", exc_info=True)
            raise RuntimeError(f"Error fetching FRED series {series_id}: {e}")

    def fetch_and_save(self, series_id, start_date, end_date, output_path):
        """Fetch a FRED series, transform to OHLCV, and save to a CSV."""
        df = self.fetch_series(series_id, start_date, end_date)
        df = self.handle_missing_data(df)
        ohlcv = self.transform_to_ohlcv(df)
        logging.info(f"Saving OHLCV data to {output_path}")
        ohlcv.to_csv(output_path, index_label="Date")
        logging.info(f"Saved OHLCV data to {output_path}")

    def handle_missing_data(self, df):
        """Handle missing data in the FRED dataset."""
        logging.info(
            f"Handling missing data using strategy: {self.missing_data_handling}"
        )
        if self.missing_data_handling == "interpolate":
            df = df.interpolate()
        elif self.missing_data_handling == "forward_fill":
            df = df.ffill()
        elif self.missing_data_handling == "flag":
            pass  # Retain missing values
        else:
            raise ValueError(
                f"Unsupported missing_data_handling: {self.missing_data_handling}"
            )
        return df

    def transform_to_ohlcv(self, df):
        """Convert single-column FRED data to OHLCV format."""
        logging.info("Transforming FRED data to OHLCV format")
        ohlcv = pd.DataFrame(
            {
                "Open": df["Value"],
                "High": df["Value"],
                "Low": df["Value"],
                "Close": df["Value"],
                "Volume": [0] * len(df),
            },
            index=df.index,
        )
        return ohlcv
