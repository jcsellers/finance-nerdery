import os

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


def test_synthetic_pipeline_with_vectorbt():
    """Run the synthetic data pipeline and validate its compatibility with vectorbt."""
    # Embedded configuration JSON
    config = {
        "tickers": {"Synthetic": ["TEST1", "TEST2"]},
        "synthetic_settings": {
            "TEST1": {
                "start_date": "2023-01-01",
                "end_date": "2023-01-10",
                "data_type": "linear",
                "start_value": 100,
                "growth_rate": 1.0,
            },
            "TEST2": {
                "start_date": "2023-01-01",
                "end_date": "2023-01-10",
                "data_type": "cash",
                "start_value": 50,
            },
        },
        "output_dir": "output/",
        "settings": {"missing_data_handling": "interpolate"},
    }

    pipeline = DataPipeline(config)
    pipeline.run()

    output_dir = config.get("output_dir", "output/")
    synthetic_tickers = config.get("tickers", {}).get("Synthetic", [])

    for ticker in synthetic_tickers:
        file_path = os.path.join(output_dir, f"{ticker}.csv")

        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"Output file for {ticker} not found at {file_path}"
            )

        print(f"Loading data for {ticker} from {file_path}...")
        df = pd.read_csv(file_path, index_col="Date", parse_dates=True)

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

        # Validate and fix the index
        df = df.loc[~df.index.duplicated(keep="first")]  # Remove duplicates
        df = df.sort_index()  # Ensure index is sorted
        df.index.freq = None  # Remove inferred frequency

        # Validate OHLCV structure
        validate_ohlcv_structure(df)

        # Test with vectorbt
        print(f"Testing vectorbt compatibility for {ticker}...")
        portfolio = vbt.Portfolio.from_signals(
            close=df["Close"],
            entries=df["Close"] > df["Close"].shift(1),
            exits=df["Close"] < df["Close"].shift(1),
            freq="D",  # Explicitly specify daily frequency
        )
        print(f"Portfolio statistics for {ticker}:\n{portfolio.stats()}")


if __name__ == "__main__":
    test_synthetic_pipeline_with_vectorbt()
