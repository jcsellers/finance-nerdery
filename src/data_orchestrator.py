import logging
import os

import pandas as pd
import toml
from dotenv import load_dotenv

from acquisition import FredAcquisition, YahooAcquisition
from merging import DataMerger
from saving import DataSaver


class Orchestrator:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.fred_api_key = os.getenv("FRED_API_KEY")
        if not self.fred_api_key:
            raise EnvironmentError("FRED_API_KEY not set in environment variables.")

    @staticmethod
    def load_config(config_path):
        """Load configuration from TOML."""
        return toml.load(config_path)

    def run(self):
        """Run the entire data pipeline."""
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )

        # Yahoo Finance Acquisition
        yahoo_config = self.config["yahoo_finance"]
        yahoo = YahooAcquisition(
            tickers=yahoo_config["tickers"],
            start_date=yahoo_config["start_date"],
            end_date=yahoo_config["end_date"],
            output_dir=self.config["paths"]["output_dir"],
        )
        yahoo_data = yahoo.fetch_data()
        yahoo.save_data(yahoo_data)

        # FRED Acquisition
        fred_config = self.config["fred_data"]
        fred = FredAcquisition(
            api_key=self.fred_api_key,
            cache_dir=self.config["paths"]["output_dir"],
            missing_data_handling=fred_config["missing_data_handling"],
        )

        fred_data = pd.DataFrame()
        for series_id in fred_config["series_ids"]:
            output_path = f"{self.config['paths']['output_dir']}/{series_id}.csv"
            fred.fetch_and_save(
                series_id=series_id,
                start_date=yahoo_config["start_date"],
                end_date=yahoo_config["end_date"],
                output_path=output_path,
            )
            # Load and append the saved series
            series_data = pd.read_csv(output_path, index_col="Date", parse_dates=True)
            fred_data = pd.concat([fred_data, series_data], axis=1)

        # Merge Datasets
        merged_data = DataMerger.merge_datasets(
            yahoo_data,
            fred_data,
            start_date=yahoo_config["start_date"],
            end_date=yahoo_config["end_date"],
        )

        # Save Merged Data
        DataSaver.save_data(merged_data, self.config["paths"]["output_dir"])


if __name__ == "__main__":
    import argparse

    # Load environment variables from .env
    load_dotenv()

    parser = argparse.ArgumentParser(description="Run the data pipeline orchestrator.")
    parser.add_argument(
        "--config", required=True, help="Path to the configuration file."
    )
    args = parser.parse_args()

    orchestrator = Orchestrator(config_path=args.config)
    orchestrator.run()
