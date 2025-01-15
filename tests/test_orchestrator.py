import json
import os

from src.backtest_orchestrator import orchestrate


def test_orchestrator_output(tmp_path):
    """Test that the orchestrator runs without errors and creates output files."""
    base_config = {
        "bundle": "custom_bundle",
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
        "output_dir": str(tmp_path / "output"),
    }
    strategy_config = {
        "strategy_name": "buy_and_hold",
        "parameters": {"target_asset": "SPY", "order_size": 100},
    }

    base_config_path = tmp_path / "base_config.json"
    strategy_config_path = tmp_path / "buy_and_hold.json"

    with open(base_config_path, "w") as base_file:
        json.dump(base_config, base_file)
    with open(strategy_config_path, "w") as strategy_file:
        json.dump(strategy_config, strategy_file)

    orchestrate(
        base_config_path=str(base_config_path),
        strategy_config_path=str(strategy_config_path),
    )

    # Assert files are created
    assert (tmp_path / "output" / "performance_metrics.csv").exists()
    assert (tmp_path / "output" / "performance_dashboard.html").exists()
