import os
from unittest.mock import MagicMock

import pandas as pd
import pytest
from fredapi import Fred

from fred_data_fetcher import FredFetcher


@pytest.fixture
def fred_test_config(tmp_path):
    return {
        "tickers": {"FRED": ["BAMLH0A0HYM2"]},
        "fred_settings": {
            "BAMLH0A0HYM2": {
                "start_date": "2023-01-01",
                "end_date": "2023-01-10",
                "alias": "OAS Spread",
            }
        },
        "output_dir": str(tmp_path),
        "missing_data_handling": "interpolate",  # Default for vectorbt compatibility
    }


def test_fetch_data_with_cache(mocker, fred_test_config):
    # Mock FRED API
    mock_fred = MagicMock()
    mock_fred.get_series.return_value = pd.Series(
        [1.5, 2.0, 2.5],
        index=pd.to_datetime(["2023-01-03", "2023-01-04", "2023-01-05"]),
    )
    mocker.patch("fred_data_fetcher.Fred", return_value=mock_fred)

    # Test fetcher
    fetcher = FredFetcher(api_key="test_key", cache_dir=fred_test_config["output_dir"])
    settings = fred_test_config["fred_settings"]["BAMLH0A0HYM2"]
    df = fetcher.fetch_data(
        "BAMLH0A0HYM2", settings["start_date"], settings["end_date"]
    )

    # Validate fetched data
    assert not df.empty, "Fetched data should not be empty."
    assert "Value" in df.columns, "Fetched data missing 'Value' column."


def test_transform_to_ohlcv(fred_test_config):
    # Sample FRED data
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2023-01-03", "2023-01-04", "2023-01-05"]),
            "Value": [1.5, 2.0, 2.5],
        }
    )
    df.set_index("Date", inplace=True)

    fetcher = FredFetcher(api_key="test_key", missing_data_handling="interpolate")
    ohlcv_df = fetcher.transform_to_ohlcv(df)

    # Validate OHLCV schema
    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
    assert list(ohlcv_df.columns) == expected_columns, "OHLCV schema mismatch."
    assert (ohlcv_df["Open"] == df["Value"]).all(), "Open values mismatch."
    assert (ohlcv_df["Close"] == df["Value"]).all(), "Close values mismatch."


def test_missing_data_handling(fred_test_config, mocker):
    # Mock FRED API
    mock_fred = MagicMock()
    mock_fred.get_series.return_value = pd.Series(
        [1.5, None, 2.5],
        index=pd.to_datetime(["2023-01-03", "2023-01-04", "2023-01-05"]),
    )
    mocker.patch("fred_data_fetcher.Fred", return_value=mock_fred)

    # Test interpolate
    fetcher = FredFetcher(api_key="test_key", missing_data_handling="interpolate")
    settings = fred_test_config["fred_settings"]["BAMLH0A0HYM2"]
    df = fetcher.fetch_data(
        "BAMLH0A0HYM2", settings["start_date"], settings["end_date"]
    )
    ohlcv_df = fetcher.transform_to_ohlcv(df)
    assert (
        ohlcv_df.isna().sum().sum() == 0
    ), "Interpolation failed to handle missing values."

    # Test forward fill
    fetcher.missing_data_handling = "forward_fill"
    ohlcv_df = fetcher.transform_to_ohlcv(df)
    assert (
        ohlcv_df.isna().sum().sum() == 0
    ), "Forward fill failed to handle missing values."

    # Test flagging
    fetcher.missing_data_handling = "flag"
    ohlcv_df = fetcher.transform_to_ohlcv(df)
    assert ohlcv_df.isna().sum().sum() > 0, "Flagging did not retain missing values."


def inspect_csv(file_path, allow_missing=False):
    """Inspect a CSV file for schema and data quality."""
    df = pd.read_csv(file_path, index_col="Date", parse_dates=True)
    print(f"Inspecting file: {file_path}")
    print(f"Columns in CSV: {list(df.columns)}")
    print(f"Missing values per column: \n{df.isna().sum()}")

    # Validate schema
    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
    assert list(df.columns) == expected_columns, f"Schema mismatch in {file_path}."

    # Check for missing values
    missing_values = df.isna().sum().sum()
    if not allow_missing:
        assert missing_values == 0, f"{file_path} contains missing values."
    else:
        print(f"File contains {missing_values} missing values as expected.")

    print(f"File passed inspection with {len(df)} rows.")
