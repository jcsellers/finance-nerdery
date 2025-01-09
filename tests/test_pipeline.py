import os
import sqlite3
from unittest.mock import patch

import pandas as pd
import pytest
from align_data import align_datasets
from create_sqlite_db import create_and_populate_unified_table
from fetch_fred_data import fetch_fred_data
from synthetic_dataset_generator import generate_cash_dataset, generate_linear_trend

# Test Constants
TEST_DB_PATH = "test_output/test_aligned_data.db"
TEST_TICKER_FILE = "test_data/test_ticker_file.csv"
TEST_ECONOMIC_DIR = "test_data/test_economic"
TEST_ALIGNED_DIR = "test_data/test_aligned"


# Setup and Teardown Fixtures
@pytest.fixture(scope="function")
def setup_test_environment():
    # Create test directories
    os.makedirs(TEST_ECONOMIC_DIR, exist_ok=True)
    os.makedirs(TEST_ALIGNED_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(TEST_DB_PATH), exist_ok=True)
    yield
    # Cleanup: Remove all files before deleting directories
    folders = [
        TEST_ECONOMIC_DIR,
        TEST_ALIGNED_DIR,
        os.path.dirname(TEST_DB_PATH),
    ]
    for folder in folders:
        for file in os.listdir(folder):
            os.remove(os.path.join(folder, file))
        os.rmdir(folder)
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)


# Test fetch_fred_data
@patch("fetch_fred_data.Fred.get_series")
def test_fetch_fred_data(mock_get_series, setup_test_environment):
    mock_data = pd.Series([1.0, 2.0], index=["2023-01-01", "2023-01-02"])
    mock_get_series.return_value = mock_data

    fred_symbols = ["TEST_FRED"]
    fetch_fred_data(fred_symbols, output_dir=TEST_ECONOMIC_DIR)

    output_file = os.path.join(TEST_ECONOMIC_DIR, "TEST_FRED.csv")
    assert os.path.exists(output_file)
    df = pd.read_csv(output_file)
    assert "Date" in df.columns and "Open" in df.columns


# Test synthetic dataset generator
def test_generate_linear_trend():
    df = generate_linear_trend()
    assert not df.empty
    assert "Date" in df.columns and "Value" in df.columns


def test_generate_cash_dataset():
    df = generate_cash_dataset()
    assert not df.empty
    assert "Date" in df.columns and "Value" in df.columns


def test_align_datasets(setup_test_environment):
    # Create mock cleaned data
    dates = pd.date_range(start="2023-01-01", end="2023-01-10")
    df = pd.DataFrame(
        {
            "Date": dates,
            "Open": 1.0,
            "Close": 1.0,
            "High": 1.0,
            "Low": 1.0,
            "Volume": 1,
        }
    )
    df.to_csv(os.path.join(TEST_ALIGNED_DIR, "TEST.csv"), index=False)

    # Run align_datasets with test-specific directories
    align_datasets(input_dir=TEST_ALIGNED_DIR, output_dir=TEST_ALIGNED_DIR)

    aligned_file = os.path.join(TEST_ALIGNED_DIR, "TEST_cleaned.csv")
    assert os.path.exists(aligned_file)


# Test database population
def test_create_sqlite_db(setup_test_environment):
    # Create a test aligned dataset
    dates = pd.date_range(start="2023-01-01", end="2023-01-10")
    df = pd.DataFrame(
        {
            "Date": dates,
            "Open": 1.0,
            "Close": 1.0,
            "High": 1.0,
            "Low": 1.0,
            "Volume": 1,
            "ticker": "TEST",
        }
    )
    test_aligned_file = os.path.join(TEST_ALIGNED_DIR, "TEST_cleaned.csv")
    df.to_csv(test_aligned_file, index=False)

    datasets = {"TEST": test_aligned_file}
    create_and_populate_unified_table(TEST_DB_PATH, datasets)

    conn = sqlite3.connect(TEST_DB_PATH)
    rows = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    conn.close()
    assert rows > 0
