import os
from unittest.mock import MagicMock

import pandas as pd
import pytest

from DataPipeline import DataPipeline


@pytest.fixture
def pipeline_test_config(tmp_path):
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
    }


def inspect_csv(file_path):
    """Inspect a CSV file for schema and data quality."""
    df = pd.read_csv(file_path, index_col="Date", parse_dates=True)

    # Validate schema
    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
    assert list(df.columns) == expected_columns, f"Schema mismatch in {file_path}."

    # Check for missing values
    missing_values = df.isna().sum().sum()
    assert missing_values == 0, f"{file_path} contains missing values."

    # Basic data properties
    assert len(df) > 0, f"{file_path} is empty."
    print(f"{file_path} passed inspection with {len(df)} rows.")


def test_pipeline_fred_integration_with_inspection(pipeline_test_config):
    pipeline = DataPipeline(pipeline_test_config)
    pipeline.run()

    output_dir = pipeline_test_config["output_dir"]
    alias = "OAS Spread"
    file_path = os.path.join(output_dir, f"{alias}.csv")

    # Validate file existence
    assert os.path.exists(file_path), f"Output file {file_path} not found."

    # Inspect the file
    inspect_csv(file_path)


def test_fred_pipeline_missing_data_handling(mocker, pipeline_test_config):
    # Mock FRED API
    mock_fred = MagicMock()
    mock_fred.get_series.return_value = pd.Series(
        [1.5, None, 2.5],
        index=pd.to_datetime(["2023-01-03", "2023-01-04", "2023-01-05"]),
    )
    mocker.patch("fred_data_fetcher.Fred", return_value=mock_fred)

    # Test interpolation
    pipeline_test_config["missing_data_handling"] = "interpolate"
    pipeline = DataPipeline(pipeline_test_config)
    pipeline.run()
    output_dir = pipeline_test_config["output_dir"]
    file_path = os.path.join(output_dir, "OAS Spread.csv")
    df = pd.read_csv(file_path, index_col="Date", parse_dates=True)
    assert df.isna().sum().sum() == 0, "Interpolation failed to handle missing data."

    # Test forward fill
    pipeline_test_config["missing_data_handling"] = "forward_fill"
    pipeline = DataPipeline(pipeline_test_config)
    pipeline.run()
    df = pd.read_csv(file_path, index_col="Date", parse_dates=True)
    assert df.isna().sum().sum() == 0, "Forward fill failed to handle missing data."

    # Test flagging
    pipeline_test_config["missing_data_handling"] = "flag"
    pipeline = DataPipeline(pipeline_test_config)
    pipeline.run()
    df = pd.read_csv(file_path, index_col="Date", parse_dates=True)
    assert df.isna().sum().sum() > 0, "Flagging did not retain missing values."
