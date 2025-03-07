import argparse
import json
import logging
from pathlib import Path

import pandas as pd
import vectorbt as vbt


class StrategyOrchestrator:
    def __init__(self, config_path, strategy_path, data_path, verbose=False):
        self.config_path = Path(config_path)
        self.strategy_path = Path(strategy_path)
        self.data_path = Path(data_path)
        self.verbose = verbose
        self.global_config = self.load_config(self.config_path)
        self.strategy_config = self.load_config(self.strategy_path)

    def load_config(self, path):
        """Load the configuration file."""
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found at {path}")
        with open(path, "r") as file:
            return json.load(file)

    def load_data(self):
        """Load and preprocess the market data for vectorbt."""
        if not self.data_path.exists():
            raise FileNotFoundError(f"Data file not found at {self.data_path}")

        logging.info(f"Loading data from {self.data_path}")
        data = pd.read_csv(self.data_path, header=[0, 1], index_col=0, parse_dates=True)

        logging.debug(f"Original columns: {data.columns}")
        # Clean column names
        data = self.clean_csv_columns(data)
        logging.debug(f"Cleaned columns: {data.columns}")

        # Verify that columns are in MultiIndex format
        if not isinstance(data.columns, pd.MultiIndex):
            raise ValueError(
                f"Expected MultiIndex columns, but got {type(data.columns)}. "
                f"Columns: {data.columns}"
            )
        return data

    @staticmethod
    def clean_csv_columns(data):
        """Clean and fix MultiIndex column names in the dataset."""
        # Sanitize each level of the MultiIndex
        cleaned_columns = []
        for col in data.columns:
            if len(col) == 2:  # Ensure it's a valid MultiIndex tuple
                cleaned_col = (
                    str(col[0]).strip(" '\"()"),  # Clean level 0
                    str(col[1]).strip(" '\"()"),  # Clean level 1
                )
                cleaned_columns.append(cleaned_col)
        data.columns = pd.MultiIndex.from_tuples(cleaned_columns)
        logging.info(f"Cleaned column names: {data.columns}")
        return data

    def run(self):
        """Run the strategy orchestrator."""
        data = self.load_data()

        # Extract the target asset from the strategy config
        if (
            "parameters" not in self.strategy_config
            or "target_asset" not in self.strategy_config["parameters"]
        ):
            raise KeyError(
                "The strategy config must include 'parameters' with 'target_asset'."
            )

        target_asset = tuple(
            self.strategy_config["parameters"]["target_asset"]
        )  # Convert list to tuple
        if target_asset not in data.columns:
            raise ValueError(
                f"Target asset {target_asset} not found in data columns. "
                f"Available columns: {list(data.columns)}"
            )

        logging.info(f"Running Buy and Hold strategy for target asset: {target_asset}")
        close_prices = data[target_asset]

        # Create the portfolio using vectorbt
        portfolio = vbt.Portfolio.from_holding(close=close_prices, freq="D")
        logging.info("Portfolio created successfully.")

        # Calculate CAGR
        start_value = portfolio.value().iloc[0]
        end_value = portfolio.value().iloc[-1]
        years = max(
            (portfolio.wrapper.index[-1] - portfolio.wrapper.index[0]).days / 365.25, 1
        )
        cagr = (end_value / start_value) ** (1 / years) - 1
        logging.info(f"CAGR calculated: {cagr:.2%}")

        stats = portfolio.stats()
        stats["CAGR [%]"] = cagr * 100
        return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run a vectorbt-enabled strategy orchestrator."
    )
    parser.add_argument(
        "--config", required=True, help="Path to the global config file."
    )
    parser.add_argument(
        "--strategy", required=True, help="Path to the strategy config file."
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
            logging.StreamHandler(),
            logging.FileHandler("logs/strategy_orchestrator.log", mode="w"),
        ],
    )

    orchestrator = StrategyOrchestrator(
        config_path=args.config,
        strategy_path=args.strategy,
        data_path=args.data,
        verbose=args.verbose,
    )
    results = orchestrator.run()

    print("Strategy Results:", results)
