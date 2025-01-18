import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay


class SyntheticPipeline:
    def __init__(self, start_date, end_date, cash_settings, linear_settings, db_conn):
        self.start_date = pd.Timestamp(start_date)
        self.end_date = pd.Timestamp(end_date)
        self.cash_settings = cash_settings
        self.linear_settings = linear_settings
        self.db_conn = db_conn
        self.nyse_calendar = CustomBusinessDay(calendar=USFederalHolidayCalendar())

    def _get_trading_days(self):
        if self.start_date >= self.end_date:
            raise ValueError("start_date must be earlier than end_date.")
        return pd.date_range(self.start_date, self.end_date, freq=self.nyse_calendar)

    def generate_cash(self):
        trading_days = self._get_trading_days()
        data = {
            "Date": trading_days,
            "value": [self.cash_settings.get("start_value", 100.0)] * len(trading_days),
            "symbol": ["synthetic_cash"] * len(trading_days),
        }
        return pd.DataFrame(data)

    def generate_linear(self):
        trading_days = self._get_trading_days()
        start_value = self.linear_settings.get("start_value", 100.0)
        growth_rate = self.linear_settings.get("growth_rate", 1.0)
        data = {
            "Date": trading_days,
            "value": [start_value + i * growth_rate for i in range(len(trading_days))],
            "symbol": ["synthetic_linear"] * len(trading_days),
        }
        return pd.DataFrame(data)

    def save_to_db(self, data):
        if self.db_conn:
            if data.empty:
                print("No data to save to the database.")
                return
            print(f"Saving {len(data)} records to the database.")

    def run(self):
        try:
            cash_data = self.generate_cash()
            linear_data = self.generate_linear()
            self.save_to_db(cash_data)
            self.save_to_db(linear_data)
        except Exception as e:
            print(f"An error occurred: {e}")
