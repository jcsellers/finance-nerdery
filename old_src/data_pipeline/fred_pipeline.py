import logging

import pandas as pd
from fredapi import Fred
from utils.sqlite_utils import clean_column_names, validate_date_ranges

# Configure logging
logger = logging.getLogger("fred_pipeline")


class FredPipeline:
    def __init__(self, start_date, end_date, tickers, aliases, api_key):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = (
            pd.to_datetime(end_date) if end_date != "current" else pd.Timestamp.today()
        )
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
                df["fred_ticker"] = ticker  # Original FRED ticker
                df["symbol"] = alias  # Alias (e.g., human-readable name)
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
