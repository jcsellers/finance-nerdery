import logging

import pandas as pd
from fredapi import Fred

from utils.sqlite_utils import clean_column_names
from utils.validation import validate_date_ranges

# Configure logging
logger = logging.getLogger("fred_pipeline")


class FredPipeline:
    def __init__(self, start_date, end_date, tickers, aliases, api_key):
        self.start_date = start_date
        self.end_date = end_date
        self.tickers = tickers
        self.aliases = aliases
        self.api_key = api_key
        self.fred = Fred(api_key=api_key)

    def run(self):
        fred_data = []
        for ticker in self.tickers:
            try:
                logger.info(f"Fetching FRED data for: {ticker}")
                data = self.fred.get_series(ticker, self.start_date, self.end_date)
                alias = self.aliases.get(ticker, ticker)
                df = pd.DataFrame(data, columns=["value"])
                df["date"] = df.index
                df["symbol"] = alias
                fred_data.append(df)
            except Exception as e:
                logger.error(f"Failed to fetch FRED data for {ticker}: {e}")

        if not fred_data:
            logger.warning("No data fetched for FRED tickers.")
            return pd.DataFrame()

        # Combine and clean data
        combined_data = pd.concat(fred_data, ignore_index=True)
        logger.info("Data combined successfully.")
        combined_data = clean_column_names(combined_data)
        combined_data = validate_date_ranges(
            combined_data, self.start_date, self.end_date
        )
        logger.info("Data cleaned and validated for FRED.")
        return combined_data
