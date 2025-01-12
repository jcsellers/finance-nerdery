from datetime import datetime, timedelta

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)


class SyntheticPipeline:
    def generate_cash(self, start_date, end_date, start_value):
        try:
            dates = pd.date_range(start=start_date, end=end_date, freq="D")
            data = pd.DataFrame({"Date": dates, "Value": [start_value] * len(dates)})
            logger.info("Synthetic cash data generated successfully.")
            return data
        except Exception as e:
            logger.error(f"Error generating synthetic cash data: {e}")
            raise

    def generate_linear(self, start_date, end_date, start_value, growth_rate):
        try:
            dates = pd.date_range(start=start_date, end=end_date, freq="D")
            values = [start_value + i * growth_rate for i in range(len(dates))]
            data = pd.DataFrame({"Date": dates, "Value": values})
            logger.info("Synthetic linear data generated successfully.")
            return data
        except Exception as e:
            logger.error(f"Error generating synthetic linear data: {e}")
            raise
