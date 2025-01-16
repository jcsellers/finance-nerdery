import json

from src.backtest_orchestrator import orchestrate


def test_output_file_creation(tmp_path):
    """Test if output files are created correctly."""
    # Define the output directory
    output_dir = tmp_path / "output" / "reports"
    base_config = {
        "bundle": "custom_bundle",
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
        "output_dir": str(output_dir),
    }
    strategy_config = {
        "strategy_name": "buy_and_hold",
        "parameters": {"target_asset": "SPY", "order_size": 100},
    }

    # Define file paths for configurations
    base_config_path = tmp_path / "base_config.json"
    strategy_config_path = tmp_path / "buy_and_hold.json"

    # Save configurations as valid JSON
    with open(base_config_path, "w") as base_file:
        json.dump(base_config, base_file)  # Correct format for JSON structure
    with open(strategy_config_path, "w") as strategy_file:
        json.dump(strategy_config, strategy_file)

    # Run the backtesting workflow
    orchestrate(
        base_config_path=str(base_config_path),
        strategy_config_path=str(strategy_config_path),
    )

    # Assert that output files are created in the specified output directory
    assert (output_dir / "performance_metrics.csv").exists(), "Metrics CSV not found"
    assert (
        output_dir / "performance_dashboard.html"
    ).exists(), "Dashboard HTML not found"
