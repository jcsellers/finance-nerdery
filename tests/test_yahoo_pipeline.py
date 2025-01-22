import os
from unittest.mock import Mock

import pandas as pd
import pytest

from yahoo_pipleline import YahooPipeline


@pytest.fixture
def yahoo_finance_settings():
    return {
        "AAPL": {"start_date": "2023-01-01", "end_date": "2023-01-10", "alias": "Apple"}
    }


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


@pytest.fixture
def output_dir(tmp_path):
    return str(tmp_path)


def test_process_yahoo_finance(mock_yahoo_fetcher, yahoo_finance_settings, output_dir):
    yahoo_pipeline = YahooPipeline(
        mock_yahoo_fetcher, output_dir, yahoo_finance_settings
    )
    yahoo_pipeline.process_yahoo_finance("AAPL")

    # Validate output file exists
    output_path = os.path.join(output_dir, "Apple.csv")
    assert os.path.exists(
        output_path
    ), "Output file for Yahoo Finance data was not created."

    # Validate file content
    df = pd.read_csv(output_path, index_col="Date", parse_dates=True)
    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
    assert list(df.columns) == expected_columns, "Output file schema mismatch."
    assert len(df) == 3, "Unexpected number of rows in Yahoo Finance output file."
