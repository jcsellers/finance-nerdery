import argparse
import json
import logging
from pathlib import Path

import pandas as pd
import vectorbt as vbt


# Vectorbt-Enabled Strategy Orchestrator
class StrategyOrchestrator:
    def __init__(self, config_path, data_path, verbose=False):
        self.config_path = Path(config_path)
        self.data_path = Path(data_path)
        self.verbose = verbose
        self.config = self.load_config()

    def load_config(self):
        """Load the configuration file for the strategy."""
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found at {self.config_path}"
            )
        with open(self.config_path, "r") as file:
            return json.load(file)

    def load_data(self):
        """Load and preprocess the market data for vectorbt."""
        if not self.data_path.exists():
            raise FileNotFoundError(f"Data file not found at {self.data_path}")

        logging.info(f"Loading data from {self.data_path}")
        data = pd.read_csv(self.data_path, header=[0, 1], index_col=0, parse_dates=True)

        # Flatten MultiIndex columns
        data.columns = ["_".join(map(str, col)).lower() for col in data.columns]
        logging.info(f"Flattened columns: {data.columns}")

        return data

    def run(self):
        """Run the strategy orchestrator using vectorbt."""
        data = self.load_data()

        # Use approximate column matching
        target_asset = self.config["parameters"]["target_asset"].lower()
        matched_column = next(
            (col for col in data.columns if target_asset in col), None
        )
        if matched_column is None:
            raise ValueError(f"Target asset {target_asset} not found in data columns.")

        logging.info(f"Running Buy and Hold strategy for: {matched_column}")

        # Use matched column for strategy
        close_prices = data[matched_column]
        portfolio = vbt.Portfolio.from_holding(close=close_prices, freq="D")

        # Calculate CAGR
        start_value = portfolio.value().iloc[0]
        end_value = portfolio.value().iloc[-1]
        years = max(
            (portfolio.wrapper.index[-1] - portfolio.wrapper.index[0]).days / 365.25, 1
        )
        cagr = (end_value / start_value) ** (1 / years) - 1

        # Return comprehensive performance stats with CAGR
        stats = portfolio.stats()
        stats["CAGR [%]"] = cagr * 100

        return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run a vectorbt-enabled strategy orchestrator."
    )
    parser.add_argument(
        "--config", required=True, help="Path to the strategy configuration file."
    )
    parser.add_argument("--data", required=True, help="Path to the market data file.")
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging for debugging."
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # Console output
            logging.FileHandler("logs/strategy_orchestrator.log", mode="w"),
        ],
    )

    orchestrator = StrategyOrchestrator(
        config_path=args.config, data_path=args.data, verbose=args.verbose
    )
    results = orchestrator.run()

    # Print results to the console
    print("Strategy Results:", results)
