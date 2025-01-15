import json
import os

import pandas as pd
import vectorbt as vbt

from src.utils.config_loader import load_config


def orchestrate(
    base_config_path="config/base_config.json",
    strategy_config_path="config/strategies/buy_and_hold.json",
):
    """
    Orchestrate the backtesting process for a given strategy using vectorbt.
    """
    # Load configurations
    config = load_config(base_config_path, strategy_config_path)
    output_dir = config.get("output_dir", "results")
    os.makedirs(output_dir, exist_ok=True)

    # Simulated price data
    price_data = pd.Series(
        [100, 101, 99, 102], index=pd.date_range(start="2025-01-01", periods=4)
    )
    initial_cash = 10000

    # Portfolio simulation using vectorbt
    portfolio = vbt.Portfolio.from_orders(
        close=price_data, size=[0, 10, 0, -10], init_cash=initial_cash
    )

    # Generate performance metrics
    sharpe = portfolio.sharpe_ratio()
    max_drawdown = portfolio.max_drawdown()

    # Save metrics
    metrics = pd.DataFrame(
        {"Metric": ["Sharpe Ratio", "Max Drawdown"], "Value": [sharpe, max_drawdown]}
    )
    metrics.to_csv(os.path.join(output_dir, "performance_metrics.csv"), index=False)

    # Save interactive dashboard
    dashboard_path = os.path.join(output_dir, "performance_dashboard.html")
    portfolio.plot().write_html(dashboard_path)

    print(f"Performance metrics saved to: {output_dir}/performance_metrics.csv")
    print(f"Dashboard saved to: {dashboard_path}")
