import os

import pandas as pd
import pytest
import vectorbt as vbt

from metrics_manager import MetricsManager


@pytest.fixture
def config(tmp_path):
    """Fixture to provide a sample configuration for MetricsManager."""
    return {"output": {"output_dir": str(tmp_path / "output")}}


@pytest.fixture
def sample_results():
    """Fixture to provide sample results with vectorbt portfolios."""
    data = pd.Series([100, 110, 105], index=pd.date_range("2023-01-01", periods=3))
    entries = pd.Series([True, False, True], index=data.index)
    exits = pd.Series([False, True, False], index=data.index)

    # Create a Portfolio with realistic signals
    portfolio = vbt.Portfolio.from_signals(close=data, entries=entries, exits=exits)

    return [
        {
            "strategy_name": "buy_and_hold",
            "portfolio": portfolio,
            "entries": entries,
            "exits": exits,
        }
    ]


def test_metrics_manager_saves_results(config, sample_results):
    """Test that MetricsManager saves vectorbt portfolio results correctly."""
    metrics_manager = MetricsManager(config)
    metrics_manager.save_results(sample_results)

    # Validate output files
    output_dir = config["output"]["output_dir"]
    strategy_dir = os.path.join(output_dir, "buy_and_hold")
    assert os.path.isdir(strategy_dir), "Strategy output directory was not created."

    # Check metrics.csv
    metrics_path = os.path.join(strategy_dir, "metrics.csv")
    assert os.path.isfile(metrics_path), "Metrics file was not created."

    # Check trade_stats.csv
    trade_stats_path = os.path.join(strategy_dir, "trade_stats.csv")
    assert os.path.isfile(trade_stats_path), "Trade stats file was not created."

    # Check signals directory
    signals_dir = os.path.join(strategy_dir, "signals")
    assert os.path.isdir(signals_dir), "Signals directory was not created."

    # Check entries and exits
    entries_path = os.path.join(signals_dir, "entries.csv")
    exits_path = os.path.join(signals_dir, "exits.csv")
    assert os.path.isfile(entries_path), "Entries file was not created."
    assert os.path.isfile(exits_path), "Exits file was not created."
