import os
from unittest.mock import patch

import pandas as pd
import pytest

from DataPipeline import DataPipeline


@pytest.fixture
def yahoo_finance_config(tmp_path):
    return {
        "tickers": {"Yahoo Finance": ["AAPL"]},
        "aliases": {"Yahoo Finance": {"AAPL": "Apple"}},
        "date_ranges": {"start_date": "2023-01-01", "end_date": "2023-01-10"},
        "settings": {"missing_data_handling": "interpolate"},
        "output_dir": str(tmp_path),
    }


@patch("yfinance.download")
def test_pipeline_yahoo_finance(mock_yf_download, yahoo_finance_config):
    # Mock Yahoo Finance API response
    mock_yf_download.return_value = pd.DataFrame(
        {
            "Open": [100, 101, 102],
            "High": [105, 106, 107],
            "Low": [99, 100, 101],
            "Close": [104, 105, 106],
            "Volume": [1000, 1100, 1200],
        },
        index=pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
    )

    pipeline = DataPipeline(yahoo_finance_config)
    pipeline.run()

    output_path = os.path.join(yahoo_finance_config["output_dir"], "Apple.csv")

    # Validate the output file exists
    assert os.path.exists(output_path), "Output file was not created."

    # Validate the content of the output file
    df = pd.read_csv(output_path, index_col="Date", parse_dates=True)
    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
    assert list(df.columns) == expected_columns, "Schema mismatch in output file."


def test_yahoo_finance_missing_data_handling(mocker, yahoo_finance_config):
    # Mock Yahoo Finance API response with missing data
    mock_yf_download = mocker.patch("yfinance.download")
    mock_yf_download.return_value = pd.DataFrame(
        {
            "Open": [100, None, 102],
            "High": [105, 106, None],
            "Low": [99, None, 101],
            "Close": [104, None, 106],
            "Volume": [1000, None, 1200],
        },
        index=pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
    )

    # Test interpolation
    yahoo_finance_config["settings"]["missing_data_handling"] = "interpolate"
    pipeline = DataPipeline(yahoo_finance_config)
    pipeline.run()
    output_path = os.path.join(yahoo_finance_config["output_dir"], "Apple.csv")
    df = pd.read_csv(output_path, index_col="Date", parse_dates=True)
    assert df.isna().sum().sum() == 0, "Interpolation failed to handle missing data."

    # Test forward fill
    yahoo_finance_config["settings"]["missing_data_handling"] = "forward_fill"
    pipeline = DataPipeline(yahoo_finance_config)
    pipeline.run()
    df = pd.read_csv(output_path, index_col="Date", parse_dates=True)
    assert df.isna().sum().sum() == 0, "Forward fill failed to handle missing data."

    # Test flagging
    yahoo_finance_config["settings"]["missing_data_handling"] = "flag"
    pipeline = DataPipeline(yahoo_finance_config)
    pipeline.run()
    df = pd.read_csv(output_path, index_col="Date", parse_dates=True)
    assert df.isna().sum().sum() > 0, "Flagging did not retain missing values."
