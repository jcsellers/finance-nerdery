import os

import pandas as pd

from fred_data_fetcher import FredFetcher
from yfinance_fetcher import YahooFinanceFetcher


class DataPipeline:
    def __init__(self, config):
        self.config = config
        self.output_dir = config.get("output_dir", "output/")
        os.makedirs(self.output_dir, exist_ok=True)

        # Initialize FRED fetcher if configuration exists
        fred_config = config.get("FRED", None)
        self.fred_fetcher = None
        if fred_config:
            self.fred_fetcher = FredFetcher(
                api_key=os.getenv(fred_config.get("api_key_env_var", "FRED_API_KEY")),
                missing_data_handling=config["settings"].get(
                    "missing_data_handling", "interpolate"
                ),
            )

        # Initialize Yahoo Finance fetcher
        self.yahoo_fetcher = YahooFinanceFetcher(
            missing_data_handling=config["settings"].get(
                "missing_data_handling", "interpolate"
            )
        )

    def run(self):
        # Process FRED tickers if present
        fred_tickers = self.config.get("tickers", {}).get("FRED", [])
        if fred_tickers and self.fred_fetcher:
            for series_id in fred_tickers:
                self.process_fred(series_id)

        # Process Yahoo Finance tickers
        yahoo_tickers = self.config.get("tickers", {}).get("Yahoo Finance", [])
        for ticker in yahoo_tickers:
            self.process_yahoo_finance(ticker)

        # Process Synthetic tickers
        synthetic_tickers = self.config.get("tickers", {}).get("Synthetic", [])
        for ticker in synthetic_tickers:
            self.process_synthetic(ticker)

    def process_fred(self, series_id):
        settings = self.config.get("fred_settings", {}).get(series_id, {})
        start_date = settings.get("start_date", "2020-01-01")
        end_date = settings.get("end_date", "current")
        alias = settings.get("alias", series_id)

        print(f"Fetching FRED data for {series_id} ({alias})...")
        try:
            df = self.fred_fetcher.fetch_data(series_id, start_date, end_date)
            ohlcv_df = self.fred_fetcher.transform_to_ohlcv(df)

            output_path = os.path.join(self.output_dir, f"{alias}.csv")
            ohlcv_df.to_csv(output_path, index_label="Date")
            print(f"Saved FRED data for {alias} to {output_path}.")
        except Exception as e:
            print(f"Error processing FRED data for {series_id}: {e}")

    def process_yahoo_finance(self, ticker):
        alias = (
            self.config.get("aliases", {}).get("Yahoo Finance", {}).get(ticker, ticker)
        )
        start_date = self.config.get("date_ranges", {}).get("start_date", "2020-01-01")
        end_date = self.config.get("date_ranges", {}).get("end_date", "current")

        print(f"Fetching Yahoo Finance data for {ticker} ({alias})...")
        try:
            df = self.yahoo_fetcher.fetch_data(ticker, start_date, end_date)

            output_path = os.path.join(self.output_dir, f"{alias}.csv")
            df.to_csv(output_path, index_label="Date")
            print(f"Saved Yahoo Finance data for {alias} to {output_path}.")
        except Exception as e:
            print(f"Error processing Yahoo Finance data for {ticker}: {e}")

    def process_synthetic(self, ticker):
        settings = self.config.get("synthetic_settings", {}).get(ticker, {})
        start_date = settings.get("start_date")
        end_date = settings.get("end_date")
        data_type = settings.get("data_type", "linear")
        start_value = settings.get("start_value", 1.0)
        growth_rate = settings.get("growth_rate", 0.01)

        print(f"Generating synthetic data for {ticker}...")
        try:
            from synthetic_data_generator import SyntheticDataGenerator

            generator = SyntheticDataGenerator(
                start_date=start_date,
                end_date=end_date,
                ticker=ticker,
                data_type=data_type,
                start_value=start_value,
                growth_rate=growth_rate,
            )
            df = generator.generate()

            output_path = os.path.join(self.output_dir, f"{ticker}.csv")
            df.to_csv(output_path, index_label="Date")
            print(f"Saved synthetic data for {ticker} to {output_path}.")
        except Exception as e:
            print(f"Error generating synthetic data for {ticker}: {e}")


if __name__ == "__main__":
    import json

    config_path = "config/fred_test_config.json"

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as config_file:
        config = json.load(config_file)

    pipeline = DataPipeline(config)
    pipeline.run()
