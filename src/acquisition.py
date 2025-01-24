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
                yf_data = vbt.YFData.download(
                    ticker.strip().upper(),
                    start=self.start_date,
                    end=self.end_date,
                )
                if yf_data is not None:
                    ticker_data = pd.DataFrame(
                        {
                            "Date": yf_data.get(
                                "Close"
                            ).index,  # Use Close to get the index
                            f"Open_{ticker}": yf_data.get("Open"),
                            f"High_{ticker}": yf_data.get("High"),
                            f"Low_{ticker}": yf_data.get("Low"),
                            f"Close_{ticker}": yf_data.get("Close"),
                            f"Volume_{ticker}": yf_data.get("Volume"),
                        }
                    )
                    ticker_data.set_index(
                        "Date", inplace=True
                    )  # Explicitly set Date as index
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
        data = data[~data.index.duplicated(keep="first")]  # Drop duplicate indices
        data = data.ffill().bfill()  # Fill missing data
        logging.debug(f"Fetched Yahoo data structure: {data.head()}")
        logging.info(f"Fetched data with valid columns: {data.columns}")
        return data

    def save_data(self, data):
        """Save Yahoo Finance data to CSV and Parquet formats."""
        csv_path = os.path.join(self.output_dir, "yahoo_data.csv")
        parquet_path = os.path.join(self.output_dir, "yahoo_data.parquet")

        data.to_csv(csv_path, index=True)
        logging.info(f"Saved Yahoo Finance data to {csv_path}")

        data.to_parquet(parquet_path, index=True)
        logging.info(f"Saved Yahoo Finance data to {parquet_path}")

    def save_data(self, data):
        """Save Yahoo Finance data to CSV and Parquet formats."""
        csv_path = os.path.join(self.output_dir, "yahoo_data.csv")
        parquet_path = os.path.join(self.output_dir, "yahoo_data.parquet")

        data.to_csv(csv_path, index=True)
        logging.info(f"Saved Yahoo Finance data to {csv_path}")

        data.to_parquet(parquet_path, index=True)
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

    def fetch_all_series(self, series_ids, start_date, end_date):
        """Fetch and structure multiple FRED series."""
        fred_data_dict = {}
        for series_id in series_ids:
            logging.info(f"Fetching FRED series: {series_id}")
            series_data = self.fetch_series(series_id, start_date, end_date)
            ohlcv_data = self.transform_to_ohlcv(series_data)
            fred_data_dict[series_id] = ohlcv_data
        return fred_data_dict

    def transform_to_ohlcv(self, df):
        """Convert single-column FRED data to OHLCV format, preserving the original Value column."""
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
        # Preserve the original 'Value' column for merging purposes
        ohlcv["Value"] = df["Value"]
        return ohlcv
