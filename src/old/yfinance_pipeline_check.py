import logging
import os

import pandas as pd
from DataPipeline import DataPipeline

# Configuration for the Yahoo Finance pipeline check
config = {
    "tickers": {"Yahoo Finance": ["AAPL"]},
    "aliases": {"Yahoo Finance": {"AAPL": "Apple"}},
    "yahoo_finance_settings": {
        "AAPL": {
            "start_date": "2023-01-01",
            "end_date": "2023-01-10",
            "alias": "Apple",
        }
    },
    "settings": {"missing_data_handling": "interpolate"},
    "output_dir": "output/",
}

# Configure logging
log_dir = os.path.join(os.path.dirname(__file__), "../logs")
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, "yfinance_pipeline_check.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file_path, mode="w"),
    ],
)
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting Yahoo Finance pipeline check...")

    pipeline = DataPipeline(config)
    pipeline.run()

    output_path = os.path.join(config["output_dir"], "Apple.csv")
    if os.path.exists(output_path):
        logger.info(f"Pipeline completed successfully. Output file: {output_path}")
        df = pd.read_csv(output_path, index_col="Date", parse_dates=True)
        logger.info(f"Output file preview:\n{df.head()}")
    else:
        logger.error("Pipeline failed to produce the expected output file.")


if __name__ == "__main__":
    main()
