import os
from unittest.mock import patch

import pandas as pd
import pytest

from DataPipeline import DataPipeline
from fred_data_fetcher import FredFetcher
from fred_pipeline import FredPipeline
from synthetic_data_generator import SyntheticDataGenerator
from synthetic_pipeline import SyntheticPipeline
from yahoo_pipleline import YahooPipeline
from yfinance_fetcher import YahooFinanceFetcher


@pytest.fixture
def fred_config(tmp_path):
    return {
        "tickers": {"FRED": ["BAMLH0A0HYM2"]},
        "fred_settings": {
            "BAMLH0A0HYM2": {
                "start_date": "2023-01-01",
                "end_date": "2023-01-10",
                "alias": "OAS_Spread",
            }
        },
        "settings": {"missing_data_handling": "interpolate"},
        "output_dir": str(tmp_path),
    }


@pytest.fixture
def yahoo_finance_config(tmp_path):
    return {
        "tickers": {"Yahoo Finance": ["AAPL"]},
        "aliases": {"Yahoo Finance": {"AAPL": "Apple"}},
        "yahoo_finance_settings": {
            "AAPL": {
                "start_date": "2023-01-01",
                "end_date": "2023-01-10",
                "alias": "Apple",
            }
        },
        "settings": {"missing_data_handling": "interpolate"},
        "output_dir": str(tmp_path),
    }


@patch("fred_data_fetcher.Fred")
def test_pipeline_fred(mock_fred, fred_config):
    # Mock FRED API response
    mock_fred_instance = mock_fred.return_value
    mock_fred_instance.get_series.return_value = pd.Series(
        [3.0, 3.1, 3.2],
        index=pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
    )

    pipeline = DataPipeline(fred_config)
    pipeline.run()

    output_path = os.path.join(fred_config["output_dir"], "OAS_Spread.csv")
    assert os.path.exists(output_path), "Output file was not created for FRED."

    df = pd.read_csv(output_path, index_col="Date", parse_dates=True)
    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
    assert list(df.columns) == expected_columns, "Schema mismatch in FRED output file."


@pytest.fixture
def mock_yahoo_fetcher():
    fetcher = Mock()
    fetcher.fetch_data.return_value = pd.DataFrame(
        {
            "Open": [100, 101, 102],
            "High": [105, 106, 107],
            "Low": [99, 100, 101],
            "Close": [104, 105, 106],
            "Volume": [1000, 1100, 1200],
        },
        index=pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
    )
    return fetcher


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
    assert os.path.exists(output_path), "Output file was not created for Yahoo Finance."

    df = pd.read_csv(output_path, index_col="Date", parse_dates=True)
    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
    assert (
        list(df.columns) == expected_columns
    ), "Schema mismatch in Yahoo Finance output file."


@pytest.mark.parametrize(
    "missing_data_handling", ["interpolate", "forward_fill", "flag"]
)
@patch("yfinance.download")
def test_yahoo_finance_missing_data(
    mock_yf_download, missing_data_handling, yahoo_finance_config
):
    # Mock Yahoo Finance API response with missing data
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

    yahoo_finance_config["settings"]["missing_data_handling"] = missing_data_handling
    pipeline = DataPipeline(yahoo_finance_config)
    pipeline.run()

    output_path = os.path.join(yahoo_finance_config["output_dir"], "Apple.csv")
    df = pd.read_csv(output_path, index_col="Date", parse_dates=True)

    if missing_data_handling == "flag":
        assert df.isna().sum().sum() > 0, "Flagging did not retain missing values."
    else:
        assert (
            df.isna().sum().sum() == 0
        ), f"{missing_data_handling} failed to handle missing data."
