import pandas as pd
from zipline.api import (
    order,
    record,
    set_commission,
    set_max_leverage,
    set_slippage,
    symbol,
)
from zipline.finance.commission import PerShare
from zipline.finance.slippage import FixedSlippage


def initialize(context):
    """
    Called once at the start of the simulation.
    Sets up the context variables and schedules functions.
    """
    context.asset = symbol(context.params["target_asset"])
    context.has_ordered = False

    # Set commission and slippage models
    set_commission(PerShare(cost=context.params.get("commission", 0.005)))
    set_slippage(FixedSlippage(spread=context.params.get("slippage", 0.01)))

    # Set maximum leverage
    set_max_leverage(context.params.get("max_leverage", 1.0))


def handle_data(context, data):
    """
    Called once per bar (minute, daily, etc.).
    Buys the target asset once at the beginning.
    """
    if not context.has_ordered:
        try:
            # Ensure price data is available
            price = data.current(context.asset, "price")
            if pd.isna(price):
                # Skip this iteration if price is NaN
                print(f"No price data available for {context.asset}. Skipping order.")
                return

            # Place the order
            order(
                context.asset,
                int(context.portfolio.cash / price),
            )
            context.has_ordered = True
            record(order_size=int(context.portfolio.cash / price))

        except KeyError as e:
            print(f"Error: Asset {context.asset} not found in data. {e}")
        except Exception as e:
            print(f"Unexpected error in handle_data: {e}")


def run_strategy(config):
    """
    Run the Buy-and-Hold strategy.

    Args:
        config (dict): Configuration for the strategy.

    Returns:
        pd.DataFrame: Backtest results.
    """
    from datetime import datetime

    from zipline import run_algorithm

    return run_algorithm(
        start=datetime.strptime(config["start_date"], "%Y-%m-%d"),
        end=datetime.strptime(config["end_date"], "%Y-%m-%d"),
        initialize=initialize,
        handle_data=handle_data,
        capital_base=config.get("capital_base", 100000),
        data_frequency="daily",
        bundle=config["bundle"],
    )
