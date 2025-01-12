import logging
import os

from dotenv import load_dotenv

from src.data_pipeline.fred_pipeline import FredPipeline
from src.data_pipeline.synthetic_pipeline import generate_cash, generate_linear
from src.data_pipeline.yahoo_pipeline import YahooPipeline
from src.utils.sqlite_utils import save_to_sqlite

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def run_yahoo_pipeline():
    yahoo_pipeline = YahooPipeline()
    tickers = ["SPY", "SSO", "UPRO", "GLD", "TLT", "^SPX", "^VIX"]
    try:
        data = yahoo_pipeline.fetch_data(tickers, "2020-01-01", "2020-12-31")
        yahoo_pipeline.save_data("data/finance_data.db", "yahoo_data", data)

        # Save data to CSV
        data.to_csv("data/csv_files/yahoo_data.csv", index=False)
        logger.info("Yahoo data saved to CSV at 'data/csv_files/yahoo_data.csv'.")

        logger.info("Yahoo Pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in Yahoo Pipeline: {e}")


def run_fred_pipeline():
    fred_api_key = os.getenv("FRED_API_KEY")
    if not fred_api_key:
        logger.error("FRED API key is not set in the environment variables.")
        return

    fred_pipeline = FredPipeline(api_key=fred_api_key)
    tickers = ["BAMLH0A0HYM2", "DGS10"]
    try:
        data = fred_pipeline.fetch_data(tickers, "2020-01-01", "2020-12-31")
        fred_pipeline.save_data("data/finance_data.db", "fred_data", data)

        # Save data to CSV
        data.to_csv("data/csv_files/fred_data.csv", index=False)
        logger.info("FRED data saved to CSV at 'data/csv_files/fred_data.csv'.")

        logger.info("FRED Pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in FRED Pipeline: {e}")


def run_synthetic_pipeline():
    try:
        cash_data = generate_cash("2020-01-01", "2020-12-31", 1000)
        linear_data = generate_linear("2020-01-01", "2020-12-31", 100, 2)

        # Save to SQLite
        save_to_sqlite("data/finance_data.db", "synthetic_cash", cash_data)
        save_to_sqlite("data/finance_data.db", "synthetic_linear", linear_data)

        # Save to CSV
        cash_data.to_csv("data/csv_files/synthetic_cash.csv", index=False)
        linear_data.to_csv("data/csv_files/synthetic_linear.csv", index=False)
        logger.info("Synthetic data saved to CSV at 'data/csv_files/'.")

        logger.info("Synthetic Pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in Synthetic Pipeline: {e}")


def run_pipeline():
    logger.info("Starting full pipeline execution.")
    run_yahoo_pipeline()
    run_fred_pipeline()
    run_synthetic_pipeline()
    logger.info("Full pipeline execution completed successfully.")


if __name__ == "__main__":
    run_pipeline()
