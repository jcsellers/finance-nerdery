import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from parser import parse_config

import pytest


def test_parse_valid_config():
    # Create a temporary valid config file
    valid_config = {
        "tickers": {"Yahoo Finance": ["SPY", "SSO"]},
        "aliases": {"Yahoo Finance": {"SPY": "SP500"}},
        "date_ranges": {"start_date": "2020-01-01", "end_date": "2023-12-31"},
        "storage": {
            "SQLite": "data/sqlite",
            "CSV": "data/csv_files",
            "output_dir": "output",
        },
    }
    config_path = "tests/test_config_valid.json"
    with open(config_path, "w") as f:
        import json

        json.dump(valid_config, f)

    config = parse_config(config_path)
    assert config["tickers"]["Yahoo Finance"] == ["SPY", "SSO"]
    assert config["storage"]["SQLite"] == "data/sqlite"
    os.remove(config_path)
