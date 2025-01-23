Strategy Orchestrator - README

Overview

The StrategyOrchestrator is a Python-based module designed to execute trading strategies using financial data processed with the vectorbt library. It supports dynamic configuration and robust performance analysis metrics, including CAGR, Sharpe Ratio, and more. The orchestrator integrates directly with real-world financial data and can handle MultiIndex column structures typical of Yahoo Finance datasets.

Features

MultiIndex Column Compatibility: Handles complex datasets with MultiIndex column formats.

Strategy Execution: Executes "Buy and Hold" and other strategies using vectorbt's efficient backtesting engine.

Performance Metrics: Calculates key metrics like CAGR, Max Drawdown, Sharpe Ratio, and more.

Dynamic Configuration: Accepts configurations via JSON files for flexible strategy specifications.

Robust Logging: Logs critical steps and errors to ensure transparency and debuggability.

Modules

1. src/strategy_orchestrator.py

Responsibilities:

Load and preprocess financial data.

Match target assets with column names in the dataset.

Execute trading strategies and calculate performance metrics.

Key Methods:

load_config:

Loads the JSON configuration file.

Raises an error if the file is missing.

load_data:

Reads the financial dataset (e.g., yahoo_data.csv) with MultiIndex columns.

Flattens MultiIndex columns into a standardized format for easier matching.

run:

Executes the configured strategy.

Matches the target asset with the dataset columns.

Uses vectorbt to run the "Buy and Hold" strategy.

Calculates and returns performance metrics, including CAGR.

Usage Example:

python src/strategy_orchestrator.py --config config/strategies/buy_and_hold_real.json --data data/csv_files/yahoo_data.csv --verbose

2. tests/test_strategy_orchestrator.py

Purpose:

Tests the functionality of StrategyOrchestrator using real or mock data.

Key Tests:

test_orchestrator_load_data:

Validates data loading from a CSV file.

Ensures the correct structure and content of columns.

test_orchestrator_run:

Executes the full strategy and validates key metrics like CAGR.

test_orchestrator_target_asset_missing:

Ensures proper error handling when the target asset is missing from the dataset.

Dependencies

Python 3.11

vectorbt

pandas

pytest

argparse

Install dependencies:

pip install -r requirements.txt

File Structure

finance-nerdery/
├── src/
│   └── strategy_orchestrator.py
├── config/
│   └── strategies/
│       └── buy_and_hold_real.json
├── data/
│   └── csv_files/
│       └── yahoo_data.csv
├── tests/
│   └── test_strategy_orchestrator.py
└── requirements.txt

Configuration Example

The orchestrator accepts a JSON configuration file specifying the strategy parameters. Example:

{
    "strategy_name": "buy_and_hold",
    "parameters": {
        "target_asset": "('spy', 'close')",
        "order_size": 100
    }
}

Common Issues

1. Target asset not found in data columns

Ensure that the target_asset in the configuration file matches the column names in the dataset.

Use --verbose to log the available column names for debugging.

2. ParserError: Header rows must have an equal number of columns.

Ensure the CSV file matches the expected MultiIndex format with consistent header rows.

Running Tests

Use pytest to run the test suite:

pytest tests/test_strategy_orchestrator.py

Future Improvements

Add support for additional strategies (e.g., SMA Crossover).

Implement dynamic asset selection.

Expand test coverage with edge cases and larger datasets.
