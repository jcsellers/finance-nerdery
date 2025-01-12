import json
import os

import pytest

from src.parser import parse_config


@pytest.fixture
def valid_config():
    return {
        "tickers": {"Yahoo Finance": ["SPY", "SSO"]},
        "aliases": {"Yahoo Finance": {"SPY": "SP500"}},
        "date_ranges": {"start_date": "2020-01-01", "end_date": "2023-12-31"},
        "storage": {
            "SQLite": "data/sqlite",
            "CSV": "data/csv_files",
            "output_dir": "output",
        },
    }


def test_parse_valid_config(valid_config):
    config_path = "tests/test_config_valid.json"
    with open(config_path, "w") as f:
        json.dump(valid_config, f)

    config = parse_config(config_path)
    assert config["tickers"]["Yahoo Finance"] == ["SPY", "SSO"]
    assert config["storage"]["SQLite"] == "data/sqlite"
    os.remove(config_path)


def test_missing_fields():
    config = {"date_ranges": {"start_date": "2020-01-01"}}
    config_path = "tests/test_config_missing_fields.json"
    with open(config_path, "w") as f:
        json.dump(config, f)

    with pytest.raises(ValueError):
        parse_config(config_path)
    os.remove(config_path)


def test_invalid_date_format(valid_config):
    valid_config["date_ranges"]["start_date"] = "01-01-2020"  # Invalid format
    config_path = "tests/test_config_invalid_date.json"
    with open(config_path, "w") as f:
        json.dump(valid_config, f)

    with pytest.raises(ValueError):
        parse_config(config_path)
    os.remove(config_path)
