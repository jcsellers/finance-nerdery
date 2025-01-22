import logging
import os

from fred_pipeline import FredPipeline
from synthetic_pipeline import SyntheticPipeline
from yahoo_pipleline import YahooPipeline


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
        fred_config = self.config.get("FRED", {})
        logging.debug(f"FRED configuration: {fred_config}")
        api_key_env_var = fred_config.get("api_key_env_var")
        if not api_key_env_var:
            logging.error(
                "FRED configuration missing 'api_key_env_var'. Check your config.json."
            )
            return

        api_key = os.getenv(api_key_env_var)
        if not api_key:
            logging.error(
                f"FRED API key not found in the environment for '{api_key_env_var}'."
            )
            return

        logging.info(f"FRED API key loaded for '{api_key_env_var}'.")
        self.fred_pipeline = FredPipeline(
            output_dir=self.output_dir,
            fred_settings=self.config.get("aliases", {}).get("FRED", {}),
            api_key=api_key,
        )
        logging.info("Initialized FRED pipeline.")

    def _initialize_yahoo_pipeline(self):
        self.yahoo_pipeline = YahooPipeline(
            output_dir=self.output_dir,
            yahoo_finance_settings=self.config.get("aliases", {}).get(
                "Yahoo Finance", {}
            ),
        )
        logging.info("Initialized Yahoo Finance pipeline.")

    def _initialize_synthetic_pipeline(self):
        self.synthetic_pipeline = SyntheticPipeline(
            output_dir=self.output_dir,
            synthetic_settings=self.config.get("synthetic_settings", {}),
        )
        logging.info("Initialized Synthetic pipeline.")

    def run(self):
        # Process FRED data
        fred_tickers = self.config["tickers"].get("FRED", [])
        if self.fred_pipeline:
            for ticker in fred_tickers:
                self.fred_pipeline.process_fred(ticker)

        # Process Yahoo Finance data
        yahoo_tickers = self.config["tickers"].get("Yahoo Finance", [])
        if self.yahoo_pipeline:
            for ticker in yahoo_tickers:
                self.yahoo_pipeline.process_yahoo_finance(ticker)

        # Process Synthetic data
        synthetic_tickers = self.config["tickers"].get("Synthetic", [])
        if self.synthetic_pipeline:
            for ticker in synthetic_tickers:
                self.synthetic_pipeline.process_synthetic(ticker)
