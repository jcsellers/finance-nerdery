import json
import os
from importlib import import_module

import pandas as pd
from zipline.data.bundles import ingest, load
from zipline.utils.run_algo import run_algorithm


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
        load(bundle_name)
    except KeyError:
        print(f"Bundle {bundle_name} not found. Ingesting...")
        ingest(bundle_name)

    # Dynamically load strategy
    strategy_name = config["strategy"]["name"]
    strategy_module = import_module(f"src.strategies.{strategy_name}")

    # Get output directory
    output_dir = config["storage"]["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    # Run backtest
    backtest_results = run_algorithm(
        start=pd.Timestamp(config["start_date"], tz="utc"),
        end=pd.Timestamp(config["end_date"], tz="utc"),
        initialize=strategy_module.initialize,
        handle_data=strategy_module.handle_data,
        analyze=strategy_module.analyze,
        bundle=bundle_name,
    )

    # Save results to output_dir
    output_file = os.path.join(output_dir, "backtest_results.csv")
    backtest_results.to_csv(output_file, index=False)
    print(f"Backtest results saved to {output_file}")

    return backtest_results
