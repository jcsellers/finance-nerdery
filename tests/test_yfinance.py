import os
from unittest.mock import patch

import pandas as pd
import pytest

from DataPipeline import DataPipeline
from yahoo_pipleline import YahooPipeline
from yfinance_fetcher import YahooFinanceFetcher


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


@patch("yfinance.download")
def test_pipeline_yahoo_finance(mock_yf_download, yahoo_finance_config):
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
    assert os.path.exists(output_path), "Output file was not created."

    df = pd.read_csv(output_path, index_col="Date", parse_dates=True)
    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
    assert list(df.columns) == expected_columns, "Schema mismatch in output file."
