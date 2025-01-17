import logging

import numpy as np
import pandas as pd

from utils.sqlite_utils import clean_column_names
from utils.validation import validate_date_ranges

# Configure logging
logger = logging.getLogger("synthetic_pipeline")


class SyntheticPipeline:
    def __init__(self, start_date, end_date, cash_settings, linear_settings):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.cash_settings = cash_settings
        self.linear_settings = linear_settings

    def generate_cash(self):
        logger.info("Generating synthetic cash data.")
        date_range = pd.date_range(self.start_date, self.end_date)
        return pd.DataFrame(
            {
                "date": date_range,
                "value": self.cash_settings["start_value"],
                "symbol": "synthetic_cash",
            }
        )

    def generate_linear(self):
        logger.info("Generating synthetic linear data.")
        date_range = pd.date_range(self.start_date, self.end_date)
        values = (
            self.linear_settings["start_value"]
            + np.arange(len(date_range)) * self.linear_settings["growth_rate"]
        )
        return pd.DataFrame(
            {"date": date_range, "value": values, "symbol": "synthetic_linear"}
        )

    def run(self):
        cash_data = self.generate_cash()
        linear_data = self.generate_linear()
        combined_data = pd.concat([cash_data, linear_data], ignore_index=True)
        logger.info("Synthetic data generated.")
        combined_data = clean_column_names(combined_data)
        combined_data = validate_date_ranges(
            combined_data, self.start_date, self.end_date
        )
        logger.info("Data cleaned and validated for Synthetic Pipeline.")
        return combined_data
