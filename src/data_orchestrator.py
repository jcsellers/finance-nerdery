import logging
import os

import pandas as pd
import pandas_market_calendars as mcal
import toml
from dotenv import load_dotenv

from acquisition import FredAcquisition, YahooAcquisition
from merging import DataMerger
from saving import DataSaver

# Load environment variables from .env
load_dotenv()


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

    def validate_dates(self, start_date, end_date):
        """Validate that the date range aligns with the NYSE trading calendar."""
        logging.info("Validating date range against NYSE trading calendar")
        nyse = mcal.get_calendar("NYSE")
        nyse_schedule = nyse.schedule(start_date=start_date, end_date=end_date)
        valid_dates = nyse_schedule.index

        if pd.Timestamp(start_date) not in valid_dates:
            adjusted_start_date = valid_dates[valid_dates >= pd.Timestamp(start_date)][
                0
            ]
            logging.warning(
                f"Start date {start_date} is not a valid trading day. Adjusted to {adjusted_start_date}."
            )
            start_date = adjusted_start_date

        if pd.Timestamp(end_date) not in valid_dates:
            adjusted_end_date = valid_dates[valid_dates <= pd.Timestamp(end_date)][-1]
            logging.warning(
                f"End date {end_date} is not a valid trading day. Adjusted to {adjusted_end_date}."
            )
            end_date = adjusted_end_date

        logging.info(f"Using adjusted date range: {start_date} to {end_date}.")
        return start_date, end_date

    def run(self):
        """Run the entire data pipeline."""
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )

        # Validate date ranges
        start_date = self.config["date_ranges"]["start_date"]
        end_date = self.config["date_ranges"]["end_date"]
        start_date, end_date = self.validate_dates(start_date, end_date)

        # Yahoo Finance Acquisition
        yahoo_config = self.config["sources"]["Yahoo_Finance"]
        yahoo = YahooAcquisition(
            tickers=yahoo_config["tickers"],
            start_date=start_date,
            end_date=end_date,
            output_dir=self.config["output"]["output_dir"],
        )
        yahoo_data = yahoo.fetch_data()
        logging.debug(f"Yahoo data after fetching: {yahoo_data.head()}")
        yahoo.save_data(yahoo_data)

        # FRED Acquisition
        fred_config = self.config["sources"]["FRED"]
        fred = FredAcquisition(
            api_key=self.fred_api_key,
            cache_dir=self.config["output"]["output_dir"],
            missing_data_handling=self.config["settings"]["missing_data_handling"],
        )
        fred_data_dict = fred.fetch_all_series(
            fred_config["series_ids"], start_date, end_date
        )
        for series_name, fred_data in fred_data_dict.items():
            logging.debug(f"FRED data preview for {series_name}: {fred_data.head()}")

        # Load saved Yahoo Finance data
        yahoo_data_path = os.path.join(
            self.config["output"]["output_dir"], "yahoo_data.csv"
        )
        yahoo_data = pd.read_csv(yahoo_data_path, index_col=0, parse_dates=True)

        # Normalize Yahoo data's index to ensure no timezone issues
        yahoo_data.index = yahoo_data.index.normalize()

        # Merge Datasets
        logging.info("Calling DataMerger.merge_datasets to align Yahoo and FRED data")
        merged_data = DataMerger.merge_datasets(yahoo_data, fred_data_dict)
        logging.debug(f"Merged data preview: {merged_data.head()}")

        # Save Merged Data
        DataSaver.validate_and_save(
            merged_data, self.config["output"]["output_dir"], name="merged_data"
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the data pipeline orchestrator.")
    parser.add_argument(
        "--config", required=True, help="Path to the configuration file."
    )
    args = parser.parse_args()

    orchestrator = Orchestrator(config_path=args.config)
    orchestrator.run()
