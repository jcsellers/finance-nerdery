import logging

import pandas as pd
import pandas_market_calendars as mcal


class DataMerger:
    @staticmethod
    def get_nyse_calendar(start_date, end_date):
        """Get NYSE trading days for a given date range."""
        nyse = mcal.get_calendar("NYSE")
        schedule = nyse.schedule(start_date=start_date, end_date=end_date)
        trading_days = schedule.index
        return pd.DataFrame(index=trading_days)

    @staticmethod
    def merge_datasets(yahoo_data, fred_data, start_date, end_date):
        """Align Yahoo Finance and FRED data using NYSE calendar."""
        logging.info("Generating NYSE trading calendar")
        nyse = mcal.get_calendar("NYSE")
        nyse_schedule = nyse.schedule(start_date=start_date, end_date=end_date)
        nyse_trading_days = nyse_schedule.index

        logging.info("Reindexing datasets to NYSE trading days")
        yahoo_data = yahoo_data.reindex(nyse_trading_days).ffill().bfill()
        fred_data = fred_data.reindex(nyse_trading_days).ffill().bfill()

        logging.info("Merging Yahoo Finance and FRED data")
        merged_data = pd.concat([yahoo_data, fred_data], axis=1)
        logging.info(f"Merged dataset shape: {merged_data.shape}")
        return merged_data
