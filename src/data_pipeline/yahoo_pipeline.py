import pandas as pd
import yfinance as yf

from ..utils.logger import get_logger

logger = get_logger(__name__)


class YahooPipeline:
    def fetch_data(self, tickers, start_date, end_date):
        """
        Fetch data from Yahoo Finance and normalize for processing.

        Args:
            tickers (list): List of tickers to fetch.
            start_date (str): Start date for the data range (YYYY-MM-DD).
            end_date (str): End date for the data range (YYYY-MM-DD).

        Returns:
            pd.DataFrame: Normalized data with columns ['date', 'ticker', 'open', 'close', ...].
        """
        try:
            # Fetch data for all tickers
            data = yf.download(
                tickers, start=start_date, end=end_date, group_by="ticker"
            )
            if data.empty:
                logger.error(f"No data fetched for tickers: {tickers}")
                raise ValueError("Empty response from Yahoo Finance.")

            # Stack and reset index
            data = data.stack(level=0, future_stack=True).reset_index()

            # Rename columns for clarity
            data.rename(columns={"level_1": "ticker", "Date": "date"}, inplace=True)

            # Extract fields and normalize
            fields = ["open", "high", "low", "close", "volume"]
            reshaped_data = pd.DataFrame()
            for field in fields:
                field_data = data[data["ticker"].str.lower() == field].copy()
                field_data.drop(columns=["ticker"], inplace=True)
                field_data.rename(columns={tickers[0]: field}, inplace=True)

                if reshaped_data.empty:
                    reshaped_data = field_data
                else:
                    reshaped_data = pd.merge(
                        reshaped_data, field_data, on="date", how="outer"
                    )

            logger.info("Yahoo Finance data normalized successfully.")
            return reshaped_data
        except Exception as e:
            logger.error(f"Error fetching Yahoo Finance data: {e}")
            raise
