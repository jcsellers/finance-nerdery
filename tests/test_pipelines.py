from unittest.mock import patch

import pandas as pd
import pytest

from src.data_pipeline.fred_pipeline import FredPipeline
from src.data_pipeline.synthetic_pipeline import generate_cash, generate_linear
from src.data_pipeline.yahoo_pipeline import YahooPipeline


@patch("yfinance.download")
def test_yahoo_pipeline_valid_data(mock_yfinance):
    mock_yfinance.return_value = pd.DataFrame(
        {
            "Date": ["2020-01-01", "2020-01-02"],
            "Open": [1, 2],
            "Close": [2, 3],
        }
    )
    pipeline = YahooPipeline()
    result = pipeline.fetch_data(["SPY"], "2020-01-01", "2020-12-31")

    # Validate lowercase column names
    assert "open" in result.columns
    assert "close" in result.columns

    # Validate data contents
    assert result["ticker"].iloc[0] == "SPY"
    assert len(result) == 2


@patch("fredapi.Fred")
def test_fred_pipeline_valid_data(mock_fred_class):
    mock_fred_instance = mock_fred_class.return_value
    mock_fred_instance.get_series.side_effect = (
        lambda ticker, observation_start, observation_end: pd.Series([1.5, 2.5, 3.5])
    )

    # Ensure a valid API key is passed to FredPipeline
    valid_api_key = "dummy_key_32_characters_long_000"
    pipeline = FredPipeline(api_key=valid_api_key)
    pipeline.fred = mock_fred_instance  # Inject the mock instance

    result = pipeline.fetch_data(["DGS10"], "2020-01-01", "2020-12-31")

    # Validate column names and data
    assert set(result.columns) == {"date", "value", "ticker", "data_flag"}
    assert result["ticker"].iloc[0] == "DGS10"
    assert len(result) == 3
    assert result["value"].tolist() == [1.5, 2.5, 3.5]
    assert result["data_flag"].tolist() == ["actual", "actual", "actual"]


def test_synthetic_pipeline():
    data_cash = generate_cash("2020-01-01", "2020-12-31", 1.0)
    data_linear = generate_linear("2020-01-01", "2020-12-31", 1.0, 0.01)

    # Validate equal lengths
    assert len(data_cash) == len(data_linear)

    # Validate constant values for cash and linear progression for linear
    assert all(data_cash["value"] == 1.0)
    assert all(
        data_linear["value"].iloc[i] == 1.0 + 0.01 * i for i in range(len(data_linear))
    )
