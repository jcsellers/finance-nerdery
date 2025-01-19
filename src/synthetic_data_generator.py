from datetime import datetime

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal


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
        # Align to NYSE calendar using pandas_market_calendars
        nyse = mcal.get_calendar("NYSE")
        schedule = nyse.schedule(start_date=self.start_date, end_date=self.end_date)
        trading_days = schedule.index

        if len(trading_days) == 0:
            raise ValueError("No valid trading days in the specified range.")

        # Generate synthetic price and volume data
        if self.data_type == "linear":
            data = self._generate_linear_data(len(trading_days))
        elif self.data_type == "cash":
            data = self._generate_cash_data(len(trading_days))
        else:
            raise ValueError("Unsupported data type. Use 'linear' or 'cash'.")

        # Create a DataFrame
        df = pd.DataFrame(
            {
                "Date": trading_days,
                "Open": data,
                "High": data,  # No random fluctuations
                "Low": data,  # No random fluctuations
                "Close": data,  # No random fluctuations
                "Volume": [
                    10000 for _ in range(len(data))
                ],  # Fixed deterministic volume
            }
        )

        # Ensure the 'Date' column is the index
        df.set_index("Date", inplace=True)

        return df

    def _generate_linear_data(self, length):
        return [self.start_value + i * self.growth_rate for i in range(length)]

    def _generate_cash_data(self, length):
        return [self.start_value for _ in range(length)]


# Example usage
generator = SyntheticDataGenerator(
    start_date="2023-01-03",
    end_date="2023-12-31",
    ticker="SYNTH",
    data_type="linear",
    start_value=100,
    growth_rate=0.5,
)

synthetic_data = generator.generate()
print(synthetic_data.head())
