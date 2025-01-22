import logging
import os

from fred_data_fetcher import FredFetcher
from fred_pipeline import FredPipeline
from synthetic_pipeline import SyntheticPipeline
from yahoo_pipleline import YahooPipeline
from yfinance_fetcher import YahooFinanceFetcher


class DataPipeline:
    def __init__(self, config):
        self.config = config
        self.output_dir = config.get("output_dir", "output/")
        os.makedirs(self.output_dir, exist_ok=True)

        # Initialize pipelines
        self.fred_pipeline = None
        self.yahoo_pipeline = None
        self.synthetic_pipeline = None

        if "FRED" in config["tickers"]:
            self._initialize_fred_pipeline()

        if "Yahoo Finance" in config["tickers"]:
            self._initialize_yahoo_pipeline()

        if "Synthetic" in config["tickers"]:
            self._initialize_synthetic_pipeline()

    def _initialize_fred_pipeline(self):
        api_key = os.getenv("FRED_API_KEY")
        if not api_key:
            logging.error(
                "FRED_API_KEY not found in the environment. Set it in your .env file or environment variables."
            )
            return

        logging.info("FRED API key loaded from environment.")
        fred_fetcher = FredFetcher(api_key=api_key)
        self.fred_pipeline = FredPipeline(
            fred_fetcher=fred_fetcher,
            output_dir=self.output_dir,
            fred_settings=self.config.get("fred_settings", {}),
        )
        logging.info("Initialized FRED pipeline.")

    def _initialize_yahoo_pipeline(self):
        yahoo_finance_settings = self.config.get("aliases", {}).get("Yahoo Finance", {})
        if not isinstance(yahoo_finance_settings, dict):
            logging.error(
                "Yahoo Finance settings must be a dictionary keyed by ticker."
            )
            return

        # Validate settings for each ticker
        for ticker, settings in yahoo_finance_settings.items():
            if not isinstance(settings, dict):
                logging.error(
                    f"Settings for ticker '{ticker}' must be a dictionary. Got: {settings}"
                )
                return

        logging.info(f"Yahoo Finance settings loaded: {yahoo_finance_settings}")
        yahoo_fetcher = YahooFinanceFetcher(
            missing_data_handling=self.config["settings"].get(
                "missing_data_handling", "interpolate"
            )
        )
        self.yahoo_pipeline = YahooPipeline(
            yahoo_fetcher=yahoo_fetcher,
            output_dir=self.output_dir,
            yahoo_finance_settings=yahoo_finance_settings,
        )
        logging.info("Initialized Yahoo Finance pipeline.")

    def _initialize_synthetic_pipeline(self):
        synthetic_settings = self.config.get("synthetic_settings", {})
        if not synthetic_settings:
            logging.error("No synthetic settings provided in configuration.")
            return

        self.synthetic_pipeline = SyntheticPipeline(
            output_dir=self.output_dir,
            synthetic_settings=synthetic_settings,
        )
        logging.info("Initialized Synthetic pipeline.")

    def run(self):
        # Process FRED data
        fred_tickers = self.config["tickers"].get("FRED", [])
        if self.fred_pipeline:
            for ticker in fred_tickers:
                if ticker not in self.config.get("fred_settings", {}):
                    logging.error(
                        f"No configuration found for FRED series ID {ticker}. Skipping."
                    )
                    continue
                self.fred_pipeline.process_fred(ticker)

        # Process Yahoo Finance data
        yahoo_tickers = self.config["tickers"].get("Yahoo Finance", [])
        if self.yahoo_pipeline:
            for ticker in yahoo_tickers:
                if ticker not in self.config["aliases"]["Yahoo Finance"]:
                    logging.error(
                        f"No configuration found for Yahoo Finance ticker '{ticker}'. Skipping."
                    )
                    continue
                self.yahoo_pipeline.process_yahoo_finance(ticker)

        # Process Synthetic data
        synthetic_tickers = self.config["tickers"].get("Synthetic", [])
        if self.synthetic_pipeline:
            for ticker in synthetic_tickers:
                if ticker not in self.config["synthetic_settings"]:
                    logging.error(
                        f"No configuration found for synthetic ticker '{ticker}'. Skipping."
                    )
                    continue
                self.synthetic_pipeline.process_synthetic(ticker)
