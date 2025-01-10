import argparse
import json
from datetime import datetime
from zipline.api import order, record, symbol
from zipline import run_algorithm


def initialize(context, asset_symbol):
    """Initialize the strategy with a configurable asset."""
    context.asset = symbol(asset_symbol)


def handle_data(context, data):
    """Execute the buy-and-hold strategy."""
    if context.portfolio.positions[context.asset].amount == 0:
        order(context.asset, 100)  # Buy 100 shares
    record(price=data.current(context.asset, "price"))


def run_buy_and_hold(
    symbol="TEST", start_date="2023-01-01", end_date="2023-01-05", capital_base=100000
):
    """Run the buy-and-hold strategy with configurable parameters."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    result = run_algorithm(
        start=start,
        end=end,
        initialize=lambda context: initialize(context, symbol),
        handle_data=handle_data,
        capital_base=capital_base,
        data_frequency="daily",
        bundle="custom_csv",
    )
    return result


def load_config(config_file="config/config.json"):
    """Load configuration from a JSON file."""
    try:
        with open(config_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error parsing configuration file: {e}")


if __name__ == "__main__":
    # CLI Argument Parsing
    parser = argparse.ArgumentParser(description="Run the Buy-and-Hold Strategy")
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.json",
        help="Path to the configuration file",
    )
    parser.add_argument("--symbol", type=str, help="Asset symbol")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--capital", type=float, help="Starting capital")
    args = parser.parse_args()

    # Load default configuration from JSON
    config = load_config(args.config)

    # Override with CLI arguments if provided
    symbol = args.symbol or config.get("symbol", "TEST")
    start_date = args.start or config.get("start_date", "2023-01-01")
    end_date = args.end or config.get("end_date", "2023-01-05")
    capital_base = args.capital or config.get("capital_base", 100000)

    # Run the strategy
    result = run_buy_and_hold(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        capital_base=capital_base,
    )
    print(result)
