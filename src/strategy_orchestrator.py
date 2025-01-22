import argparse
import json
import logging
from pathlib import Path

import pandas as pd

# Debugging: Confirm module and constructor loading
print(f"Loading StrategyOrchestrator from {__file__}")

# BaseStrategy class to ensure standardization for all strategies
class BaseStrategy:
    def initialize(self, config):
        """
        Initialize the strategy with specific configuration parameters.
        :param config: dict containing strategy-specific configuration.
        """
        raise NotImplementedError("initialize method not implemented")

    def run(self, data):
        """
        Process the data to generate trading signals.
        :param data: DataFrame containing market data.
        """
        raise NotImplementedError("run method not implemented")

    def output(self):
        """
        Return the results or signals generated by the strategy.
        """
        raise NotImplementedError("output method not implemented")


# Buy-and-Hold Strategy implementation
class BuyAndHoldStrategy(BaseStrategy):
    def __init__(self):
        self.target_asset = None
        self.order_size = None
        self.holdings = []

    def initialize(self, config):
        self.target_asset = config.get("target_asset")
        self.order_size = config.get(
            "order_size", 100
        )  # Default to 100 units if not specified

    def run(self, data):
        if self.target_asset not in data.columns:
            raise ValueError(f"Target asset {self.target_asset} not in data columns.")

        # Simulate a buy at the first available price
        first_price = data[self.target_asset].iloc[0]
        self.holdings.append(
            {"asset": self.target_asset, "price": first_price, "units": self.order_size}
        )
        logging.info(
            f"Bought {self.order_size} units of {self.target_asset} at {first_price}"
        )

    def output(self):
        return self.holdings


# Cash-Linear Alternation Strategy implementation
class CashLinearAlternationStrategy(BaseStrategy):
    def __init__(self):
        self.linear_asset = None
        self.cash_asset = None
        self.order_size = None
        self.interval = None
        self.holdings = []

    def initialize(self, config):
        self.linear_asset = config.get("linear_asset")
        self.cash_asset = config.get("cash_asset")
        self.order_size = config.get("order_size", 100)
        self.interval = config.get("interval", 1)  # Default to switching every period

    def run(self, data):
        if self.linear_asset not in data.columns or self.cash_asset not in data.columns:
            raise ValueError("Linear or cash asset not in data columns.")

        self.holdings = []
        for i, date in enumerate(data.index):
            if i % self.interval == 0:
                self.holdings.append(
                    {
                        "asset": self.cash_asset,
                        "price": data.loc[date, self.cash_asset],
                        "units": self.order_size,
                    }
                )
            else:
                self.holdings.append(
                    {
                        "asset": self.linear_asset,
                        "price": data.loc[date, self.linear_asset],
                        "units": self.order_size,
                    }
                )
            logging.info(
                f"Alternated to {self.holdings[-1]['asset']} at {self.holdings[-1]['price']} on {date}"
            )

    def output(self):
        return self.holdings


# Strategy Orchestrator
class StrategyOrchestrator:
    def __init__(self, config_path, data_path):
        print(
            f"Initializing StrategyOrchestrator with config_path={config_path}, data_path={data_path}"
        )
        self.config_path = Path(config_path)
        self.data_path = Path(data_path)
        self.strategy = None

    def load_config(self):
        """Load the configuration file for the strategy."""
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found at {self.config_path}"
            )
        with open(self.config_path, "r") as file:
            return json.load(file)

    def load_data(self):
        """Load the market data from the specified path."""
        if not self.data_path.exists():
            raise FileNotFoundError(f"Data file not found at {self.data_path}")

        # Determine the appropriate reader based on file content or config
        try:
            if "fred" in str(self.data_path).lower():
                from fred_data_reader import FredDataReader

                reader = FredDataReader(self.data_path)
                reader.load_data()
                ticker = self.load_config()["parameters"]["target_asset"]
                return reader.get_ticker_data(ticker)

            elif "yahoo" in str(self.data_path).lower():
                from yahoo_data_reader import YahooDataReader

                reader = YahooDataReader(self.data_path)
                reader.load_data()
                ticker = self.load_config()["parameters"]["target_asset"].lower()
                return reader.get_ticker_data(ticker)

            # Convert Path to string explicitly
            else:
                return pd.read_csv(
                    str(self.data_path), parse_dates=["date"], index_col="date"
                )
        except Exception as e:
            raise ValueError(f"Error reading data: {e}")

    def run(self):
        """Run the strategy orchestrator."""
        config = self.load_config()
        strategy_name = config["strategy_name"]
        parameters = config["parameters"]

        if strategy_name == "buy_and_hold":
            self.strategy = BuyAndHoldStrategy()
        elif strategy_name == "cash_linear_alternation":
            self.strategy = CashLinearAlternationStrategy()
        else:
            raise ValueError(f"Unsupported strategy: {strategy_name}")

        self.strategy.initialize(parameters)
        data = self.load_data()
        self.strategy.run(data)
        return self.strategy.output()


# Set up logging
logging.basicConfig(
    filename="logs/strategy_orchestrator.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Example execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a strategy orchestrator.")
    parser.add_argument(
        "--config", required=True, help="Path to the strategy configuration file."
    )
    parser.add_argument("--data", required=True, help="Path to the market data file.")
    args = parser.parse_args()

    orchestrator = StrategyOrchestrator(config_path=args.config, data_path=args.data)
    results = orchestrator.run()
    print("Strategy Results:", results)
