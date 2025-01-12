import os
from unittest.mock import MagicMock, patch

import pytest
from dotenv import load_dotenv

from src.data_pipeline.fred_pipeline import FredPipeline
from src.data_pipeline.synthetic_pipeline import SyntheticPipeline
from src.data_pipeline.yahoo_pipeline import YahooPipeline

# Load environment variables from the .env file
load_dotenv()


@patch("yfinance.download")
def test_yahoo_pipeline_valid_data(mock_yfinance):
    import pandas as pd

    mock_yfinance.return_value = pd.DataFrame(
        {"Date": ["2020-01-01", "2020-01-02"], "Open": [1, 2], "Close": [2, 3]}
    )
    pipeline = YahooPipeline()
    result = pipeline.fetch_data(["SPY"], "2020-01-01", "2020-12-31")
    # Adjusted to match the actual column names
    assert "open" in [col.lower() for col in result.columns]
    assert "close" in [col.lower() for col in result.columns]


@patch("fredapi.Fred")
def test_fred_pipeline_valid_data(mock_fred_class):
    import pandas as pd

    # Mock the Fred instance and its methods
    mock_fred_instance = mock_fred_class.return_value
    mock_fred_instance.get_series.side_effect = (
        lambda ticker, observation_start, observation_end: pd.Series([1.5, 2.5, 3.5])
    )

    # Use the API key from environment variables
    from dotenv import load_dotenv

    load_dotenv()
    api_key = os.getenv("FRED_API_KEY")
    assert api_key is not None, "Environment variable FRED_API_KEY is not set"

    # Instantiate FredPipeline and inject the mock
    pipeline = FredPipeline(api_key=api_key)
    pipeline.fred = (
        mock_fred_instance  # Override the `fred` attribute with the mock instance
    )

    # Fetch data using the mocked pipeline
    result = pipeline.fetch_data(["DGS10"], "2020-01-01", "2020-12-31")

    # Verify the structure of the DataFrame
    expected_columns = {"date", "value", "ticker", "data_flag"}
    assert (
        set(result.columns) == expected_columns
    ), f"Unexpected columns: {result.columns.tolist()}"

    # Ensure the 'ticker' column contains 'DGS10'
    assert result["ticker"].unique().tolist() == ["DGS10"]

    # Validate row count and content
    assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
    assert result["value"].tolist() == [1.5, 2.5, 3.5]
    assert result["data_flag"].tolist() == ["actual", "actual", "actual"]


def test_synthetic_pipeline():
    pipeline = SyntheticPipeline()
    data_cash = pipeline.generate_cash("2020-01-01", "2020-12-31", 1.0)
    data_linear = pipeline.generate_linear("2020-01-01", "2020-12-31", 1.0, 0.01)
    assert len(data_cash) == len(data_linear)
