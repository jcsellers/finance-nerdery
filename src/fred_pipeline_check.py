import json
import os

import pandas as pd

from DataPipeline import DataPipeline
from yfinance_fetcher import YahooFinanceFetcher

# Load the FRED test configuration
config_path = os.path.abspath("./config/fred_test_config.json")

# Check if the config file exists
if not os.path.exists(config_path):
    raise FileNotFoundError(f"Config file not found at {config_path}")

with open(config_path, "r") as config_file:
    config = json.load(config_file)

# Ensure output directory exists
output_dir = config.get("output_dir", "output/")
os.makedirs(output_dir, exist_ok=True)

# Run the pipeline
pipeline = DataPipeline(config)
pipeline.run()

# Analyze output for FRED
for ticker, alias in config["aliases"]["FRED"].items():
    csv_path = os.path.join(output_dir, f"{alias}.csv")

    if os.path.exists(csv_path):
        # Load OHLCV data
        ohlcv_df = pd.read_csv(csv_path, index_col="Date", parse_dates=True)

        # Normalize the index and rebuild it without frequency
        ohlcv_df.index = pd.to_datetime(ohlcv_df.index).normalize()
        ohlcv_df.index = pd.DatetimeIndex(
            ohlcv_df.index.values, freq=None
        )  # Explicitly remove frequency

        # Remove duplicate indices and validate data
        ohlcv_df = ohlcv_df.loc[~ohlcv_df.index.duplicated(keep="first")]

        # Handle missing values
        print("Rows with NaN values before handling:")
        print(ohlcv_df[ohlcv_df.isna().any(axis=1)])

        # Interpolate and handle edge NaNs with forward/backward fill
        ohlcv_df = ohlcv_df.interpolate(method="linear").ffill().bfill()

        # Ensure correct data types
        ohlcv_df = ohlcv_df.astype(
            {
                "Open": "float64",
                "High": "float64",
                "Low": "float64",
                "Close": "float64",
                "Volume": "int64",
            }
        )

        # Save and reload data to confirm format
        temp_csv_path = "temp_ohlcv.csv"
        ohlcv_df.to_csv(temp_csv_path, index_label="Date")

        # Debug the saved CSV to ensure the correct format
        with open(temp_csv_path, "r") as file:
            print("Content of saved CSV:")
            print(file.read())

        ohlcv_df = pd.read_csv(temp_csv_path, index_col="Date", parse_dates=True)

        # Validate no remaining NaN values
        assert (
            ohlcv_df.index.is_monotonic_increasing
        ), "Index must be monotonic increasing"
        assert not ohlcv_df.isna().any().any(), "Data contains NaN values"

        # Display the first few rows of the data
        print("Rows after handling NaNs:")
        print(ohlcv_df.head())

        # Optionally, analyze with vectorbt
        try:
            import vectorbt as vbt

            # Debug index details before analysis
            print("Index details before vectorbt analysis:")
            print(ohlcv_df.index)
            print(ohlcv_df.head())
            print(ohlcv_df.info())

            # Rebuild index without frequency and fully reset metadata
            ohlcv_df.index = pd.to_datetime(ohlcv_df.index, errors="coerce")
            ohlcv_df.index = pd.DatetimeIndex(ohlcv_df.index.values, freq=None)

            print("Index after processing:", ohlcv_df.index)

            # Create a portfolio using vectorbt
            portfolio = vbt.Portfolio.from_signals(
                close=ohlcv_df["Close"],
                entries=ohlcv_df["Close"]
                > ohlcv_df["Close"].shift(1),  # Example entry condition
                exits=ohlcv_df["Close"]
                < ohlcv_df["Close"].shift(1),  # Example exit condition
            )
            print(portfolio.stats())  # Use stats() for portfolio analysis
            portfolio.plot().show()
        except ImportError:
            print("vectorbt is not installed. Skipping portfolio analysis.")
        except Exception as e:
            print(f"Error analyzing data with vectorbt: {e}")
    else:
        print(f"Output file {csv_path} not found. Ensure the pipeline ran correctly.")

# Analyze output for Yahoo Finance
for ticker, alias in config["aliases"]["Yahoo Finance"].items():
    csv_path = os.path.join(output_dir, f"{alias}.csv")

    if os.path.exists(csv_path):
        # Load OHLCV data
        ohlcv_df = pd.read_csv(csv_path, index_col="Date", parse_dates=True)

        # Normalize the index and rebuild it without frequency
        ohlcv_df.index = pd.to_datetime(ohlcv_df.index).normalize()
        ohlcv_df.index = pd.DatetimeIndex(
            ohlcv_df.index.values, freq=None
        )  # Explicitly remove frequency

        # Remove duplicate indices and validate data
        ohlcv_df = ohlcv_df.loc[~ohlcv_df.index.duplicated(keep="first")]

        # Handle missing values
        print("Rows with NaN values before handling:")
        print(ohlcv_df[ohlcv_df.isna().any(axis=1)])

        # Interpolate and handle edge NaNs with forward/backward fill
        ohlcv_df = ohlcv_df.interpolate(method="linear").ffill().bfill()

        # Ensure correct data types
        ohlcv_df = ohlcv_df.astype(
            {
                "Open": "float64",
                "High": "float64",
                "Low": "float64",
                "Close": "float64",
                "Volume": "int64",
            }
        )

        # Save and reload data to confirm format
        temp_csv_path = "temp_ohlcv_yahoo.csv"
        ohlcv_df.to_csv(temp_csv_path, index_label="Date")

        # Debug the saved CSV to ensure the correct format
        with open(temp_csv_path, "r") as file:
            print("Content of saved CSV:")
            print(file.read())

        ohlcv_df = pd.read_csv(temp_csv_path, index_col="Date", parse_dates=True)

        # Validate no remaining NaN values
        assert (
            ohlcv_df.index.is_monotonic_increasing
        ), "Index must be monotonic increasing"
        assert not ohlcv_df.isna().any().any(), "Data contains NaN values"

        # Display the first few rows of the data
        print("Rows after handling NaNs:")
        print(ohlcv_df.head())

        # Optionally, analyze with vectorbt
        try:
            import vectorbt as vbt

            # Debug index details before analysis
            print("Index details before vectorbt analysis:")
            print(ohlcv_df.index)
            print(ohlcv_df.head())
            print(ohlcv_df.info())

            # Rebuild index without frequency and fully reset metadata
            ohlcv_df.index = pd.to_datetime(ohlcv_df.index, errors="coerce")
            ohlcv_df.index = pd.DatetimeIndex(ohlcv_df.index.values, freq=None)

            print("Index after processing:", ohlcv_df.index)

            # Create a portfolio using vectorbt
            portfolio = vbt.Portfolio.from_signals(
                close=ohlcv_df["Close"],
                entries=ohlcv_df["Close"]
                > ohlcv_df["Close"].shift(1),  # Example entry condition
                exits=ohlcv_df["Close"]
                < ohlcv_df["Close"].shift(1),  # Example exit condition
            )
            print(portfolio.stats())  # Use stats() for portfolio analysis
            portfolio.plot().show()
        except ImportError:
            print("vectorbt is not installed. Skipping portfolio analysis.")
        except Exception as e:
            print(f"Error analyzing data with vectorbt: {e}")
    else:
        print(f"Output file {csv_path} not found. Ensure the pipeline ran correctly.")
