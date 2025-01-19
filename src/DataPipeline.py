import os

import pandas as pd
import pandas_market_calendars as mcal

from fred_data_fetcher import FredFetcher
from synthetic_data_generator import SyntheticDataGenerator


class DataPipeline:
    def __init__(self, config):
        self.config = config
        self.missing_data_handling = config.get("missing_data_handling", "interpolate")
        self.fred_fetcher = FredFetcher(
            api_key=os.getenv("FRED_API_KEY"),
            missing_data_handling=self.missing_data_handling,
        )

    # Other methods remain the same

    def run(self):
        for source, tickers in self.config["tickers"].items():
            for ticker in tickers:
                if source == "Synthetic":
                    self.process_synthetic(ticker)
                elif source == "FRED":
                    self.process_fred(ticker)
                else:
                    print(f"Unsupported source: {source}")

    def process_fred(self, series_id):
        settings = self.config["fred_settings"].get(series_id, {})
        start_date = settings.get("start_date", "2020-01-01")
        end_date = settings.get("end_date", "current")
        alias = settings.get("alias", series_id)

        # Fetch data
        df = self.fred_fetcher.fetch_data(series_id, start_date, end_date)

        # Transform to OHLCV format
        ohlcv_df = self.fred_fetcher.transform_to_ohlcv(df)

        # Save to CSV
        output_dir = self.config["output_dir"]
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{alias}.csv")
        ohlcv_df.to_csv(output_path)
        print(f"Saved FRED data for {alias} to {output_path}")

    def process_synthetic(self, ticker):
        settings = self.config["synthetic_settings"].get(ticker, {})
        generator = SyntheticDataGenerator(
            start_date=settings.get("start_date", "2023-01-01"),
            end_date=settings.get("end_date", "2023-12-31"),
            ticker=ticker,
            data_type=settings.get("data_type", "linear"),
            start_value=settings.get("start_value", 100),
            growth_rate=settings.get("growth_rate", 0.5),
        )
        df = generator.generate()
        output_dir = self.config["output_dir"]
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{ticker}.csv")
        df.to_csv(output_path)
        print(f"Saved synthetic data for {ticker} to {output_path}")
