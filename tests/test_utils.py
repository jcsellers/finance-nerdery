import sys

sys.path.insert(0, "/mnt/data/finance-nerdery/src")

import os

import pytest

from src.utils.validation import (
    validate_aliases,
    validate_date_ranges,
    validate_paths,
    validate_tickers,
)


def test_validate_tickers():
    with pytest.raises(ValueError):
        validate_tickers({})
    validate_tickers({"Yahoo Finance": ["SPY", "SSO"]})


def test_validate_aliases():
    with pytest.raises(ValueError):
        validate_aliases({"SPY": "alias1", "SSO": "alias1"})  # Duplicate aliases
    validate_aliases({"SPY": "alias1", "SSO": "alias2"})


def test_validate_date_ranges():
    with pytest.raises(ValueError):
        validate_date_ranges(
            {"start_date": "2023-12-31", "end_date": "2020-01-01"}
        )  # start > end
    validate_date_ranges({"start_date": "2020-01-01", "end_date": "2023-12-31"})


def test_validate_paths():
    test_dir = "tests/temp_dir"
    validate_paths({"SQLite": test_dir})
    assert os.path.exists(test_dir)
    os.rmdir(test_dir)
