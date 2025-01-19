import json
import os

import pandas as pd

from DataPipeline import DataPipeline

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

# Analyze output
for ticker, alias in config["aliases"]["FRED"].items():
    csv_path = os.path.join(output_dir, f"{alias}.csv")

    if os.path.exists(csv_path):
        # Load OHLCV data
        ohlcv_df = pd.read_csv(csv_path, index_col="Date", parse_dates=True)

        # Display the first few rows of the data
        print(ohlcv_df.head())

        # Optionally, analyze with vectorbt
        try:
            import vectorbt as vbt

            # Create a portfolio using vectorbt
            portfolio = vbt.Portfolio.from_signals(
                close=ohlcv_df["Close"],
                entries=ohlcv_df["Close"]
                > ohlcv_df["Close"].shift(1),  # Example entry condition
                exits=ohlcv_df["Close"]
                < ohlcv_df["Close"].shift(1),  # Example exit condition
            )
            print(portfolio.total_return())
            portfolio.plot().show()
        except ImportError:
            print("vectorbt is not installed. Skipping portfolio analysis.")
        except Exception as e:
            print(f"Error analyzing data with vectorbt: {e}")
    else:
        print(f"Output file {csv_path} not found. Ensure the pipeline ran correctly.")
