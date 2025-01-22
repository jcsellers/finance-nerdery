import logging
from datetime import datetime

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

logger = logging.getLogger(__name__)


class SyntheticDataGenerator:
    def __init__(
        self,
        start_date,
        end_date,
        ticker,
        data_type="linear",
        start_value=1.0,
        growth_rate=0.01,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.ticker = ticker
        self.data_type = data_type
        self.start_value = start_value
        self.growth_rate = growth_rate

    def generate(self):
        nyse = mcal.get_calendar("NYSE")
        schedule = nyse.schedule(start_date=self.start_date, end_date=self.end_date)
        trading_days = schedule.index

        if len(trading_days) == 0:
            raise ValueError("No valid trading days in the specified range.")
        logger.info(
            f"Generated {len(trading_days)} trading days from {self.start_date} to {self.end_date}"
        )

        if self.data_type == "linear":
            data = self._generate_linear_data(len(trading_days))
        elif self.data_type == "cash":
            data = self._generate_cash_data(len(trading_days))
        else:
            raise ValueError("Unsupported data type. Use 'linear' or 'cash'.")

        logger.info(f"Generated data for {len(data)} points.")

        df = pd.DataFrame(
            {
                "Date": trading_days,
                "Open": data,
                "High": data,
                "Low": data,
                "Close": data,
                "Volume": [10000] * len(data),
            }
        )
        df.set_index("Date", inplace=True)
        logger.info(f"Generated DataFrame with {len(df)} rows.")
        return df

    def _generate_linear_data(self, length):
        return [self.start_value + i * self.growth_rate for i in range(length)]

    def _generate_cash_data(self, length):
        return [self.start_value for _ in range(length)]
