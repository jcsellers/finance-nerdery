import os
from unittest.mock import Mock

import pandas as pd
import pytest

from fred_pipeline import FredPipeline


@pytest.fixture
def fred_settings():
    return {
        "BAMLH0A0HYM2": {
            "start_date": "2023-01-01",
            "end_date": "2023-01-10",
            "alias": "OAS_Spread",
        }
    }


@pytest.fixture
def mock_fred_fetcher():
    fetcher = Mock()
    fetcher.fetch_data.return_value = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "Value": [3.0, 3.1, 3.2],
        }
    ).set_index("Date")
    fetcher.transform_to_ohlcv.return_value = pd.DataFrame(
        {
            "Open": [3.0, 3.1, 3.2],
            "High": [3.0, 3.1, 3.2],
            "Low": [3.0, 3.1, 3.2],
            "Close": [3.0, 3.1, 3.2],
            "Volume": [0, 0, 0],
        },
        index=pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
    )
    return fetcher


@pytest.fixture
def output_dir(tmp_path):
    return str(tmp_path)


def test_process_fred(mock_fred_fetcher, fred_settings, output_dir):
    fred_pipeline = FredPipeline(mock_fred_fetcher, output_dir, fred_settings)
    fred_pipeline.process_fred("BAMLH0A0HYM2")

    # Validate output file exists
    output_path = os.path.join(output_dir, "OAS_Spread.csv")
    assert os.path.exists(output_path), "Output file for FRED data was not created."

    # Validate file content
    df = pd.read_csv(output_path, index_col="Date", parse_dates=True)
    assert list(df.columns) == [
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
    ], "Output file schema mismatch."
    assert len(df) == 3, "Unexpected number of rows in FRED output file."
