import logging
import os

from fred_pipeline import FredPipeline
from synthetic_pipeline import SyntheticPipeline
from yahoo_pipleline import YahooPipeline

# Configure logging
log_dir = os.path.join(os.path.dirname(__file__), "../logs")
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, "data_pipeline.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file_path, mode="w")],
)
logger = logging.getLogger(__name__)

from fred_data_fetcher import FredFetcher
from yfinance_fetcher import YahooFinanceFetcher


class DataPipeline:
    def __init__(self, config):
        logger.debug("Initializing DataPipeline...")
        self.config = config
        self.output_dir = config.get("output_dir", "output/")
        os.makedirs(self.output_dir, exist_ok=True)

        # Initialize FRED pipeline
        fred_config = config.get("FRED", {})
        self.fred_pipeline = None
        if fred_config:
            api_key_env_var = fred_config.get("api_key_env_var")
            if api_key_env_var:
                api_key = os.getenv(api_key_env_var)
                if api_key:
                    fred_fetcher = FredFetcher(
                        api_key,
                        missing_data_handling=config["settings"].get(
                            "missing_data_handling", "interpolate"
                        ),
                    )
                    self.fred_pipeline = FredPipeline(
                        fred_fetcher, self.output_dir, config.get("fred_settings", {})
                    )
                    logger.debug("FredPipeline initialized successfully.")
                else:
                    logger.error(f"Environment variable {api_key_env_var} not set.")
            else:
                logger.error("FRED configuration missing API key environment variable.")
        else:
            logger.warning("No FRED configuration provided.")

        # Initialize Yahoo Finance pipeline
        yahoo_fetcher = YahooFinanceFetcher(
            missing_data_handling=config["settings"].get(
                "missing_data_handling", "interpolate"
            )
        )
        self.yahoo_pipeline = YahooPipeline(
            yahoo_fetcher=yahoo_fetcher,
            output_dir=self.output_dir,
            yahoo_finance_settings=config.get("yahoo_finance_settings", {}),
        )
        logger.debug("YahooPipeline initialized successfully.")

        # Initialize Synthetic pipeline
        self.synthetic_pipeline = SyntheticPipeline(
            output_dir=self.output_dir,
            synthetic_settings=config.get("synthetic_settings", {}),
        )
        logger.debug("SyntheticPipeline initialized successfully.")

    def run(self):
        logger.info("Starting the data pipeline...")

        # Process FRED tickers
        fred_tickers = self.config.get("tickers", {}).get("FRED", [])
        if not fred_tickers or not self.fred_pipeline:
            logger.warning(
                "No FRED tickers to process or FredPipeline not initialized. Check configuration and API key."
            )
        else:
            for series_id in fred_tickers:
                self.fred_pipeline.process_fred(series_id)

        # Process Yahoo Finance tickers
        yahoo_tickers = self.config.get("tickers", {}).get("Yahoo Finance", [])
        for ticker in yahoo_tickers:
            self.yahoo_pipeline.process_yahoo_finance(ticker)

        # Process Synthetic tickers
        synthetic_tickers = self.config.get("tickers", {}).get("Synthetic", [])
        for ticker in synthetic_tickers:
            self.synthetic_pipeline.process_synthetic(ticker)

        logger.info("Data pipeline completed.")
