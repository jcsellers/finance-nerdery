import logging
import os


class YahooPipeline:
    def __init__(self, yahoo_fetcher, output_dir, yahoo_finance_settings):
        """
        Initialize the YahooPipeline.

        :param yahoo_fetcher: Instance of YahooFinanceFetcher.
        :param output_dir: Directory where output files will be saved.
        :param yahoo_finance_settings: Dictionary of Yahoo Finance-specific settings.
        """
        self.yahoo_fetcher = yahoo_fetcher
        self.output_dir = output_dir
        self.yahoo_finance_settings = yahoo_finance_settings

    def process_yahoo_finance(self, ticker):
        """
        Process a Yahoo Finance ticker by fetching, transforming, and saving the data.

        :param ticker: Yahoo Finance ticker symbol.
        """
        logger = logging.getLogger(__name__)
        logger.debug(f"Entering process_yahoo_finance for ticker: {ticker}")

        settings = self.yahoo_finance_settings.get(ticker, {})
        if not settings:
            logger.error(
                f"No configuration found for ticker {ticker} in yahoo_finance_settings. Skipping."
            )
            return

        start_date = settings.get("start_date", "2020-01-01")
        end_date = settings.get("end_date", "current")
        alias = settings.get("alias", ticker)

        logger.info(
            f"Fetching Yahoo Finance data for {ticker} ({alias}) from {start_date} to {end_date}."
        )
        try:
            df = self.yahoo_fetcher.fetch_data(ticker, start_date, end_date)
            logger.debug(f"Fetched data for {ticker}:\n{df.head()}")

            # Ensure index is properly set
            df.index.name = "Date"

            output_path = os.path.join(self.output_dir, f"{alias}.csv")
            logger.info(f"Saving Yahoo Finance data for {alias} to {output_path}.")
            df.to_csv(output_path, index_label="Date")
            logger.info(f"Saved Yahoo Finance data for {alias} to {output_path}.")
        except Exception as e:
            logger.error(
                f"Error processing Yahoo Finance data for {ticker}: {e}", exc_info=True
            )
