import logging
import os

import pandas as pd
import pandas_market_calendars as mcal
import vectorbt as vbt
from dotenv import load_dotenv

from DataPipeline import DataPipeline

# Inline configuration for testing
config = {
    "FRED": {"api_key_env_var": "FRED_API_KEY"},
    "tickers": {"FRED": ["BAMLH0A0HYM2"], "Yahoo Finance": [], "Synthetic": []},
    "fred_settings": {
        "BAMLH0A0HYM2": {
            "start_date": "1997-01-01",
            "end_date": "current",
            "alias": "OAS_Spread",
        }
    },
    "output_dir": "output/",
    "settings": {"missing_data_handling": "interpolate"},
}

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

# Configure logging
log_dir = os.path.join(os.path.dirname(__file__), "../logs")
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, "fred_pipeline_check.log")
logging.basicConfig(
    level=logging.INFO,  # Set to INFO or WARNING for less verbosity in production
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file_path, mode="w")],
)
logger = logging.getLogger(__name__)


def validate_ohlcv_structure(df):
    """Ensure the OHLCV data has the correct structure for vectorbt."""
    required_columns = ["Open", "High", "Low", "Close", "Volume"]
    for column in required_columns:
        if column not in df.columns:
            logger.error(f"Missing required column: {column}")
            raise ValueError(f"Missing required column: {column}")

    # Validate data types
    if not pd.api.types.is_float_dtype(df["Close"]):
        logger.error("'Close' column must be of type float64.")
        raise TypeError("'Close' column must be of type float64.")
    if not pd.api.types.is_integer_dtype(df["Volume"]):
        logger.error("'Volume' column must be of type int64.")
        raise TypeError("'Volume' column must be of type int64.")

    # Validate index
    if not isinstance(df.index, pd.DatetimeIndex):
        logger.error("Index must be a DatetimeIndex.")
        raise TypeError("Index must be a DatetimeIndex.")
    if not df.index.is_monotonic_increasing:
        logger.error("Index must be monotonic increasing.")
        raise ValueError("Index must be monotonic increasing.")

    logger.info("OHLCV structure validation passed.")


def test_fred_pipeline_with_vectorbt():
    """Run the FRED data pipeline and validate its compatibility with vectorbt."""

    # Ensure FRED_API_KEY is set
    fred_api_key = os.getenv("FRED_API_KEY")
    if fred_api_key:
        logger.info("FRED_API_KEY loaded successfully.")
    else:
        logger.error(
            "FRED_API_KEY is not set in the environment. Please check your .env file."
        )
        return

    # Log configuration
    logger.info("Starting FRED pipeline with the following configuration:")
    logger.info(config)

    # Adjust the date range to valid trading days
    fred_settings = config.get("fred_settings", {}).get("BAMLH0A0HYM2", {})
    start_date = fred_settings.get("start_date", "1997-01-01")
    end_date = fred_settings.get("end_date", "current")

    # Resolve "current" date to today's date
    if end_date == "current":
        end_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    # Align start_date and end_date to valid NYSE trading days
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=start_date, end_date=end_date)

    if schedule.empty:
        logger.error(f"No valid trading days between {start_date} and {end_date}.")
        return

    aligned_start_date = schedule.index[0].strftime("%Y-%m-%d")
    aligned_end_date = schedule.index[-1].strftime("%Y-%m-%d")

    fred_settings["start_date"] = aligned_start_date
    fred_settings["end_date"] = aligned_end_date

    logger.info(
        f"Resolved date range: start_date={aligned_start_date}, end_date={aligned_end_date}"
    )

    # Use a persistent temporary directory for testing
    temp_dir = os.path.join(os.path.dirname(__file__), "../temp_output")
    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f"Using persistent temporary directory: {temp_dir}")
    config["output_dir"] = temp_dir

    pipeline = DataPipeline(config)

    try:
        logger.info("Running the data pipeline...")
        pipeline.run()
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)

    fred_tickers = config.get("tickers", {}).get("FRED", [])

    logger.info(f"Tickers to process: {fred_tickers}")

    for ticker in fred_tickers:
        alias = config.get("fred_settings", {}).get(ticker, {}).get("alias", ticker)
        file_path = os.path.join(temp_dir, f"{alias}.csv")

        logger.info(f"Looking for file: {file_path}")

        if not os.path.exists(file_path):
            logger.warning(f"Output file not found for {ticker}. Skipping processing.")
            continue

        logger.info(f"Loading data for {alias} from {file_path}...")
        df = pd.read_csv(file_path, index_col="Date", parse_dates=True)

        # Log DataFrame information
        logger.info(f"Data loaded for {alias} with {len(df)} rows.")

        # Validate and fix the index
        df = df.loc[~df.index.duplicated(keep="first")]  # Remove duplicates
        df = df.sort_index()  # Ensure index is sorted
        df.index.freq = None  # Remove inferred frequency

        # Log transformed DataFrame head
        logger.info(f"Transformed data for {alias} (first rows):\n{df.head()}")

        # Ensure all columns are cast to the correct types
        df = df.astype(
            {
                "Open": "float64",
                "High": "float64",
                "Low": "float64",
                "Close": "float64",
                "Volume": "int64",
            }
        )

        # Validate OHLCV structure
        validate_ohlcv_structure(df)

        # Test with vectorbt
        logger.info(f"Testing vectorbt compatibility for {alias}...")
        portfolio = vbt.Portfolio.from_signals(
            close=df["Close"],
            entries=df["Close"] > df["Close"].shift(1),
            exits=df["Close"] < df["Close"].shift(1),
            freq="D",  # Explicitly specify daily frequency
        )
        logger.info(f"Portfolio statistics for {alias}:\n{portfolio.stats()}")


if __name__ == "__main__":
    test_fred_pipeline_with_vectorbt()
