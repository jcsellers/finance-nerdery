import json
import os

import pandas as pd
import quantstats as qs  # Added QuantStats for performance analysis
from dateutil.tz import UTC
from zipline import run_algorithm
from zipline.data import bundles


def orchestrate(config_path):
    """
    Orchestrate the end-to-end backtesting workflow.
    """
    # Load configuration
    with open(config_path, "r") as f:
        config = json.load(f)

    # Ensure the custom bundle is ingested
    bundle_name = config["bundle"]
    try:
        bundles.load(bundle_name)
    except KeyError:
        print(f"Bundle {bundle_name} not found. Ingesting...")
        bundles.ingest(bundle_name)

    # Dynamically load strategy
    strategy_name = config["strategy"]["name"]
    strategy_module = __import__(f"src.strategies.{strategy_name}")

    # Get output directory
    output_dir = config["storage"]["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    # Run backtest
    backtest_results = run_algorithm(
        start=pd.Timestamp(config["start_date"]).tz_localize(
            None
        ),  # Ensure timezone-naive dates
        end=pd.Timestamp(config["end_date"]).tz_localize(None),
        initialize=strategy_module.initialize,
        handle_data=strategy_module.handle_data,
        analyze=strategy_module.analyze,
        bundle=bundle_name,
        capital_base=config.get("capital_base", 1e6),
    )

    # Generate QuantStats report
    returns = backtest_results["portfolio_value"].pct_change().dropna()
    qs.reports.html(returns, output=os.path.join(output_dir, "quantstats_report.html"))
    print(f"QuantStats report saved to {output_dir}/quantstats_report.html")

    # Save results
    output_path = os.path.join(output_dir, "backtest_results.csv")
    backtest_results.to_csv(output_path)
    print(f"Backtest results saved to {output_path}")


# Additional fix for NumPy compatibility
import numpy as np

if hasattr(np, "NINF"):
    delattr(np, "NINF")  # Ensure np.NINF is safely removed globally if present
    np.NINF = -np.inf
