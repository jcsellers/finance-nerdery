import os

import pandas as pd

from synthetic_data_generator import SyntheticDataGenerator


class DataPipeline:
    def __init__(self, config):
        self.config = config

    def run(self):
        for source, tickers in self.config["tickers"].items():
            for ticker in tickers:
                if source == "Synthetic":
                    self.process_synthetic(ticker)
                else:
                    print(f"Unsupported source: {source}")

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


if __name__ == "__main__":
    config = {
        "tickers": {"Synthetic": ["TEST1", "TEST2"]},
        "synthetic_settings": {
            "TEST1": {
                "start_date": "2023-01-01",
                "end_date": "2023-06-30",
                "data_type": "linear",
                "start_value": 50,
                "growth_rate": 1,
            },
            "TEST2": {
                "start_date": "2023-01-01",
                "end_date": "2023-06-30",
                "data_type": "cash",
                "start_value": 100,
            },
        },
        "output_dir": "src/output",
    }

    pipeline = DataPipeline(config)
    pipeline.run()
