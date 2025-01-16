import logging
import sqlite3
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


def generate_cash(start_date, end_date, constant_value):
    dates = pd.date_range(start=start_date, end=end_date)
    data = {
        "date": dates,
        "value": [constant_value] * len(dates),
        "type": "cash",
    }
    return pd.DataFrame(data)


def generate_linear(start_date, end_date, start_value, slope):
    dates = pd.date_range(start=start_date, end=end_date)
    values = [start_value + slope * i for i in range(len(dates))]
    data = {
        "date": dates,
        "value": values,
        "type": "linear",
    }
    return pd.DataFrame(data)


class SyntheticPipeline:
    def __init__(self, start_date, end_date, cash_settings, linear_settings):
        self.start_date = start_date
        self.end_date = end_date
        self.cash_settings = cash_settings
        self.linear_settings = linear_settings

    def run(self):
        """
        Run the synthetic data pipeline.
        Returns:
            dict: Dictionary containing 'cash' and 'linear' DataFrames.
        """
        data_cash = generate_cash(
            self.start_date, self.end_date, self.cash_settings["start_value"]
        )
        data_linear = generate_linear(
            self.start_date,
            self.end_date,
            self.linear_settings["start_value"],
            self.linear_settings["growth_rate"],
        )
        logger.info("Synthetic data generated.")
        return {"cash": data_cash, "linear": data_linear}

    def save_data(self, output_dir):
        """
        Save synthetic data to CSV files.
        Args:
            output_dir (str): Directory to save the synthetic data CSV files.
        """
        synthetic_data = self.run()
        for data_type, df in synthetic_data.items():
            file_path = f"{output_dir}/synthetic_{data_type}.csv"
            df.to_csv(file_path, index=False)
            logger.info(f"Synthetic {data_type} data saved to {file_path}.")

    def save_to_database(self, db_path):
        """
        Save synthetic data to an SQLite database.
        Args:
            db_path (str): Path to the SQLite database.
        """
        synthetic_data = self.run()
        combined_data = pd.concat(synthetic_data.values(), ignore_index=True)
        try:
            logger.info(f"Saving synthetic data to database: {db_path}")
            conn = sqlite3.connect(db_path)
            combined_data.to_sql(
                "synthetic_data", conn, if_exists="append", index=False
            )
            conn.close()
            logger.info("Synthetic data saved to database successfully.")
        except Exception as e:
            logger.error(f"Error saving synthetic data to database: {e}")
            raise
