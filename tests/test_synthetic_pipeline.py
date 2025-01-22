import os
from unittest.mock import Mock

import pandas as pd
import pytest

from synthetic_pipeline import SyntheticPipeline


@pytest.fixture
def synthetic_settings():
    return {
        "TEST1": {
            "start_date": "2023-01-01",
            "end_date": "2023-01-05",
            "data_type": "linear",
            "start_value": 50,
            "growth_rate": 0.02,
        }
    }


@pytest.fixture
def mock_synthetic_generator(mocker):
    mock_generator = mocker.patch("synthetic_pipeline.SyntheticDataGenerator")
    mock_generator.return_value.generate.return_value = pd.DataFrame(
        {
            "Date": pd.date_range(start="2023-01-01", periods=5),
            "Open": [50, 51, 52, 53, 54],
            "High": [51, 52, 53, 54, 55],
            "Low": [49, 50, 51, 52, 53],
            "Close": [50.5, 51.5, 52.5, 53.5, 54.5],
            "Volume": [0, 0, 0, 0, 0],
        }
    ).set_index("Date")
    return mock_generator


@pytest.fixture
def output_dir(tmp_path):
    return str(tmp_path)


def test_process_synthetic(mock_synthetic_generator, synthetic_settings, output_dir):
    synthetic_pipeline = SyntheticPipeline(output_dir, synthetic_settings)
    synthetic_pipeline.process_synthetic("TEST1")

    # Validate output file exists
    output_path = os.path.join(output_dir, "TEST1.csv")
    assert os.path.exists(
        output_path
    ), "Output file for synthetic data was not created."

    # Validate file content
    df = pd.read_csv(output_path, index_col="Date", parse_dates=True)
    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
    assert list(df.columns) == expected_columns, "Output file schema mismatch."
    assert len(df) == 5, "Unexpected number of rows in synthetic output file."
