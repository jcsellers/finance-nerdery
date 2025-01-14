import json
import os
from unittest.mock import ANY, MagicMock, patch

import pandas as pd
import pytest

from src.backtest_orchestrator import orchestrate
from src.strategies.buy_and_hold import handle_data, initialize


@pytest.fixture
def config_path(tmp_path, overrides=None):
    """Create a temporary configuration file with optional overrides."""
    config = {
        "bundle": "custom_bundle",
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
        "strategy": {"name": "buy_and_hold"},
        "analysis_library": "pyfolio",
        "storage": {"output_dir": str(tmp_path)},
    }
    if overrides:
        config.update(overrides)

    config_file = tmp_path / "config.json"
    with open(config_file, "w") as f:
        json.dump(config, f)
    return str(config_file)


@patch("src.backtest_orchestrator.load")
@patch("src.backtest_orchestrator.ingest")
@patch("src.backtest_orchestrator.run_algorithm")
@patch("src.backtest_orchestrator.import_module")
def test_orchestrator_loads_and_runs(
    mock_import_module, mock_run_algorithm, mock_ingest, mock_load, config_path
):
    """Test the orchestrator loads and runs the backtest."""
    # Mock the imported strategy module
    mock_strategy = MagicMock()
    mock_import_module.return_value = mock_strategy

    # Simulate bundle not being ingested
    mock_load.side_effect = KeyError("Bundle not found")

    # Run the orchestrator
    orchestrate(config_path)

    # Validate ingestion was triggered
    mock_ingest.assert_called_once_with("custom_bundle")

    # Validate strategy module was imported
    mock_import_module.assert_called_once_with("src.strategies.buy_and_hold")

    # Validate run_algorithm was executed
    mock_run_algorithm.assert_called_once()
    kwargs = mock_run_algorithm.call_args.kwargs
    assert kwargs["initialize"] == mock_strategy.initialize
    assert kwargs["handle_data"] == mock_strategy.handle_data
    assert kwargs["analyze"] == mock_strategy.analyze


@patch("pyfolio.create_full_tear_sheet")
def test_pyfolio_analysis(mock_pyfolio, tmp_path, config_path):
    """Test Pyfolio integration and validate inputs."""
    # Mock performance data
    perf = pd.DataFrame({"portfolio_value": [1000, 1010, 1020]})
    perf["returns"] = perf["portfolio_value"].pct_change().dropna()

    # Mock analyze function in the strategy
    with patch("src.strategies.buy_and_hold.analyze", return_value=None):
        orchestrate(config_path)

    # Validate Pyfolio is called with correct returns
    mock_pyfolio.assert_called_once_with(returns=perf["returns"])


@patch("src.strategies.buy_and_hold.record")
@patch("src.strategies.buy_and_hold.order")
def test_buy_and_hold_strategy_edge_cases(mock_order, mock_record):
    """Test Buy-and-Hold strategy under edge conditions."""
    mock_context = MagicMock()
    mock_context.asset = "SPY"

    # Case 1: Already holding the asset
    mock_context.has_ordered = True
    mock_data = MagicMock()
    handle_data(mock_context, mock_data)
    mock_order.assert_not_called()

    # Case 2: Low cash balance
    mock_context.has_ordered = False
    mock_context.portfolio.cash = 1  # Not enough for 100 shares
    handle_data(mock_context, mock_data)
    mock_order.assert_not_called()


@patch("src.strategies.buy_and_hold.record")
@patch("src.strategies.buy_and_hold.order")
def test_buy_and_hold_strategy_logic(mock_order, mock_record):
    """Test Buy-and-Hold strategy order and recording logic."""
    mock_context = MagicMock()
    mock_context.asset = "SPY"
    mock_context.has_ordered = False
    mock_data = MagicMock()

    # Initialize and handle data
    initialize(mock_context)
    handle_data(mock_context, mock_data)

    # Validate order placement
    mock_order.assert_called_once_with(mock_context.asset, 100)
    assert mock_context.has_ordered, "Order flag should be set after placing the order."

    # Validate price recording
    mock_record.assert_called_once_with(price=mock_data.current.return_value)


def test_output_file_creation(tmp_path, config_path):
    """Test if output files are correctly created in the output_dir."""
    output_dir = tmp_path / "output"
    os.makedirs(output_dir, exist_ok=True)

    overrides = {"storage": {"output_dir": str(output_dir)}}
    config_file = config_path(tmp_path, overrides)

    orchestrate(config_file)

    # Check that the output file exists
    output_file = output_dir / "backtest_results.csv"
    assert os.path.exists(
        output_file
    ), "Expected output file not found: backtest_results.csv"
