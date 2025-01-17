import json

import pytest
from src.utils.config_loader import load_config


def test_config_loader_merging(tmp_path):
    """
    Test that the configuration loader correctly merges configurations.
    """
    base_config = {
        "bundle": "custom_bundle",
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
    }
    strategy_config = {
        "strategy_name": "buy_and_hold",
        "parameters": {"target_asset": "SPY", "order_size": 100},
    }

    # Create temporary config files
    base_config_path = tmp_path / "base_config.json"
    strategy_config_path = tmp_path / "buy_and_hold.json"

    with open(base_config_path, "w") as base_file:
        json.dump(base_config, base_file)

    with open(strategy_config_path, "w") as strategy_file:
        json.dump(strategy_config, strategy_file)

    # Load and merge configs
    merged_config = load_config(base_config_path, strategy_config_path)

    # Assertions to validate merging
    assert merged_config["bundle"] == "custom_bundle"
    assert merged_config["start_date"] == "2025-01-01"
    assert merged_config["parameters"]["target_asset"] == "SPY"
    assert merged_config["parameters"]["order_size"] == 100
    assert merged_config["strategy_name"] == "buy_and_hold"
