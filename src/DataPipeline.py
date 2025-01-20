import logging
import os

import pandas as pd

from fred_data_fetcher import FredFetcher
from yfinance_fetcher import YahooFinanceFetcher

# Configure logging
log_dir = os.path.join(os.path.dirname(__file__), "../logs")
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, "pipeline.log")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file_path, mode="w")],
)
logger = logging.getLogger(__name__)


class DataPipeline:
    def __init__(self, config):
        logger.debug("Initializing DataPipeline...")
        self.config = config
        self.output_dir = config.get("output_dir", "output/")
        os.makedirs(self.output_dir, exist_ok=True)
        logger.debug(f"Output directory set to: {self.output_dir}")

        # Initialize FRED fetcher if configuration exists
        fred_config = config.get("FRED", None)
        self.fred_fetcher = None
        if fred_config:
            api_key_env_var = fred_config.get("api_key_env_var")
            if api_key_env_var:
                api_key = os.getenv(api_key_env_var)
                if api_key:
                    self.fred_fetcher = FredFetcher(
                        api_key=api_key,
                        missing_data_handling=config["settings"].get(
                            "missing_data_handling", "interpolate"
                        ),
                    )
                    logger.debug(
                        f"FredFetcher initialized with API key from env var: {api_key_env_var}"
                    )
                else:
                    logger.error(
                        f"Environment variable {api_key_env_var} not set. FredFetcher will not be initialized."
                    )
            else:
                logger.error(
                    "No API key environment variable specified in FRED configuration."
                )
        else:
            logger.warning(
                "No FRED configuration found. Skipping FRED fetcher initialization."
            )

        # Initialize Yahoo Finance fetcher
        logger.debug("Initializing YahooFinanceFetcher...")
        self.yahoo_fetcher = YahooFinanceFetcher(
            missing_data_handling=config["settings"].get(
                "missing_data_handling", "interpolate"
            )
        )

    def run(self):
        logger.info("Starting the data pipeline...")

        # Process FRED tickers if present
        fred_tickers = self.config.get("tickers", {}).get("FRED", [])
        logger.debug(f"FRED tickers to process: {fred_tickers}")
        if fred_tickers and self.fred_fetcher:
            for series_id in fred_tickers:
                self.process_fred(series_id)
        else:
            logger.warning("No FRED tickers to process or FredFetcher not initialized.")

        # Process Yahoo Finance tickers
        yahoo_tickers = self.config.get("tickers", {}).get("Yahoo Finance", [])
        logger.debug(f"Yahoo Finance tickers to process: {yahoo_tickers}")
        for ticker in yahoo_tickers:
            self.process_yahoo_finance(ticker)

        # Process Synthetic tickers
        synthetic_tickers = self.config.get("tickers", {}).get("Synthetic", [])
        logger.debug(f"Synthetic tickers to process: {synthetic_tickers}")
        for ticker in synthetic_tickers:
            self.process_synthetic(ticker)

        logger.info("Data pipeline completed.")

    def process_fred(self, series_id):
        logger.debug(f"Entering process_fred for series_id: {series_id}")
        settings = self.config.get("fred_settings", {}).get(series_id, {})
        logger.debug(f"FRED settings for {series_id}: {settings}")

        start_date = settings.get("start_date", "2020-01-01")
        end_date = settings.get("end_date", "current")
        alias = settings.get("alias", series_id)

        logger.info(
            f"Fetching FRED data for {series_id} (alias: {alias}), start_date: {start_date}, end_date: {end_date}..."
        )
        try:
            df = self.fred_fetcher.fetch_data(series_id, start_date, end_date)
            logger.debug(
                f"Fetched data for {series_id}: {df.head() if not df.empty else 'No data fetched'}"
            )
            if df.empty:
                logger.warning(
                    f"No data available for series_id: {series_id}. Skipping."
                )
                return

            logger.info(f"Transforming FRED data for {series_id}...")
            ohlcv_df = self.fred_fetcher.transform_to_ohlcv(df)
            logger.debug(f"Transformed data for {series_id}: {ohlcv_df.head()}")

            output_path = os.path.join(self.output_dir, f"{alias}.csv")
            logger.info(f"Saving FRED data for {alias} to {output_path}")
            self.fred_fetcher.save_to_csv(ohlcv_df, output_path)

            logger.info(f"Saved FRED data for {alias} to {output_path}")
        except Exception as e:
            logger.error(
                f"Error processing FRED data for {series_id}: {e}", exc_info=True
            )
        logger.debug(f"Exiting process_fred for series_id: {series_id}")

    def process_synthetic(self, ticker):
        logger.debug(f"Entering process_synthetic for ticker: {ticker}")
        settings = self.config.get("synthetic_settings", {}).get(ticker, {})
        start_date = settings.get("start_date")
        end_date = settings.get("end_date")
        data_type = settings.get("data_type", "linear")
        start_value = settings.get("start_value", 1.0)
        growth_rate = settings.get("growth_rate", 0.01)

        logger.info(
            f"Generating synthetic data for {ticker} with start_date={start_date}, end_date={end_date}, data_type={data_type}, growth_rate={growth_rate}"
        )
        try:
            from synthetic_data_generator import SyntheticDataGenerator

            generator = SyntheticDataGenerator(
                start_date=start_date,
                end_date=end_date,
                ticker=ticker,
                data_type=data_type,
                start_value=start_value,
                growth_rate=growth_rate,
            )
            df = generator.generate()
            logger.debug(f"Generated synthetic data for {ticker}: {df.head()}")

            output_path = os.path.join(self.output_dir, f"{ticker}.csv")
            logger.info(f"Saving synthetic data for {ticker} to {output_path}")
            df.to_csv(output_path, index_label="Date")
            logger.info(f"Saved synthetic data for {ticker} to {output_path}")
        except Exception as e:
            logger.error(
                f"Error generating synthetic data for {ticker}: {e}", exc_info=True
            )
        logger.debug(f"Exiting process_synthetic for ticker: {ticker}")


def process_yahoo_finance(self, ticker):
    logger.debug(f"Entering process_yahoo_finance for ticker: {ticker}")
    alias = self.config.get("aliases", {}).get("Yahoo Finance", {}).get(ticker, ticker)
    start_date = self.config.get("date_ranges", {}).get("start_date", "2020-01-01")
    end_date = self.config.get("date_ranges", {}).get("end_date", "current")

    logger.info(
        f"Fetching Yahoo Finance data for {ticker} with alias={alias}, start_date={start_date}, end_date={end_date}"
    )
    try:
        df = self.yahoo_fetcher.fetch_data(ticker, start_date, end_date)
        logger.debug(f"Fetched Yahoo Finance data for {ticker}: {df.head()}")

        output_path = os.path.join(self.output_dir, f"{alias}.csv")
        logger.info(f"Saving Yahoo Finance data for {alias} to {output_path}")
        df.to_csv(output_path, index_label="Date")
        logger.info(f"Saved Yahoo Finance data for {alias} to {output_path}")
    except Exception as e:
        logger.error(
            f"Error processing Yahoo Finance data for {ticker}: {e}", exc_info=True
        )
    logger.debug(f"Exiting process_yahoo_finance for ticker: {ticker}")


if __name__ == "__main__":
    import json

    config_path = "config/fred_test_config.json"

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as config_file:
        config = json.load(config_file)

    pipeline = DataPipeline(config)
    pipeline.run()
