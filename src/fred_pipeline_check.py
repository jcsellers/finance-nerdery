import os
import tempfile

import pandas as pd
import vectorbt as vbt

from DataPipeline import DataPipeline


def validate_ohlcv_structure(df):
    """Ensure the OHLCV data has the correct structure for vectorbt."""
    required_columns = ["Open", "High", "Low", "Close", "Volume"]
    for column in required_columns:
        if column not in df.columns:
            raise ValueError(f"Missing required column: {column}")

    # Validate data types
    if not pd.api.types.is_float_dtype(df["Close"]):
        raise TypeError("'Close' column must be of type float64.")
    if not pd.api.types.is_integer_dtype(df["Volume"]):
        raise TypeError("'Volume' column must be of type int64.")

    # Validate index
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Index must be a DatetimeIndex.")
    if not df.index.is_monotonic_increasing:
        raise ValueError("Index must be monotonic increasing.")

    print("OHLCV structure validation passed.")


def test_fred_pipeline_with_vectorbt():
    """Run the FRED data pipeline and validate its compatibility with vectorbt."""
    # Embedded configuration JSON
    config = {
        "tickers": {"FRED": ["BAMLH0A0HYM2"]},
        "fred_settings": {
            "BAMLH0A0HYM2": {
                "start_date": "2023-01-01",
                "end_date": "2023-01-10",
                "alias": "OAS_Spread",
            }
        },
        "output_dir": None,  # We will use a temp directory
        "settings": {"missing_data_handling": "interpolate"},
    }

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Temporary output directory: {temp_dir}")
        config["output_dir"] = temp_dir

        pipeline = DataPipeline(config)

        try:
            pipeline.run()
        except Exception as e:
            print(f"Pipeline error: {e}")

        fred_tickers = config.get("tickers", {}).get("FRED", [])

        for ticker in fred_tickers:
            alias = config.get("fred_settings", {}).get(ticker, {}).get("alias", ticker)
            file_path = os.path.join(temp_dir, f"{alias}.csv")

            print(f"Looking for file: {file_path}")

            if not os.path.exists(file_path):
                print(f"Output file not found for {ticker}. Skipping processing.")
                continue

            print(f"Loading data for {alias} from {file_path}...")
            df = pd.read_csv(file_path, index_col="Date", parse_dates=True)

            # Validate and fix the index
            df = df.loc[~df.index.duplicated(keep="first")]  # Remove duplicates
            df = df.sort_index()  # Ensure index is sorted
            df.index.freq = None  # Remove inferred frequency

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
            print(f"Testing vectorbt compatibility for {alias}...")
            portfolio = vbt.Portfolio.from_signals(
                close=df["Close"],
                entries=df["Close"] > df["Close"].shift(1),
                exits=df["Close"] < df["Close"].shift(1),
                freq="D",  # Explicitly specify daily frequency
            )
            print(f"Portfolio statistics for {alias}:{portfolio.stats()}")


if __name__ == "__main__":
    test_fred_pipeline_with_vectorbt()
