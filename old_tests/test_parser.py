import json
import os
import tempfile

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
    """Test parsing a valid configuration file."""
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_config:
        json.dump(valid_config, temp_config)
        temp_config_path = temp_config.name

    try:
        config = parse_config(temp_config_path)
        assert config["tickers"]["Yahoo Finance"] == ["SPY", "SSO"]
        assert config["storage"]["SQLite"] == "data/sqlite"
    finally:
        os.remove(temp_config_path)


def test_missing_fields():
    """Test parsing a configuration file with missing fields."""
    config = {"date_ranges": {"start_date": "2020-01-01"}}
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_config:
        json.dump(config, temp_config)
        temp_config_path = temp_config.name

    try:
        with pytest.raises(ValueError):
            parse_config(temp_config_path)
    finally:
        os.remove(temp_config_path)


def test_invalid_date_format(valid_config):
    """Test parsing a configuration file with invalid date formats."""
    valid_config["date_ranges"]["start_date"] = "01-01-2020"  # Invalid format
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_config:
        json.dump(valid_config, temp_config)
        temp_config_path = temp_config.name

    try:
        with pytest.raises(ValueError):
            parse_config(temp_config_path)
    finally:
        os.remove(temp_config_path)
