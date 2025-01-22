import os

import pandas as pd
import pytest

from DataPipeline import DataPipeline


@pytest.fixture
def synthetic_test_config(tmp_path):
    return {
        "tickers": {"Synthetic": ["TEST1", "TEST2"]},
        "synthetic_settings": {
            "TEST1": {
                "start_date": "2023-01-01",
                "end_date": "2023-01-05",
                "data_type": "linear",
                "start_value": 100,
                "growth_rate": 1,
            },
            "TEST2": {
                "start_date": "2023-01-01",
                "end_date": "2023-01-05",
                "data_type": "cash",
                "start_value": 50,
            },
        },
        "output_dir": str(tmp_path),
        "settings": {"missing_data_handling": "interpolate"},
    }


def test_synthetic_pipeline_execution(synthetic_test_config):
    pipeline = DataPipeline(synthetic_test_config)
    pipeline.run()

    output_dir = synthetic_test_config["output_dir"]
    expected_files = ["TEST1.csv", "TEST2.csv"]

    for file_name in expected_files:
        file_path = os.path.join(output_dir, file_name)
        assert os.path.exists(file_path), f"{file_name} not found in output directory."


def test_synthetic_csv_content(synthetic_test_config):
    pipeline = DataPipeline(synthetic_test_config)
    pipeline.run()

    output_dir = synthetic_test_config["output_dir"]
    for ticker, settings in synthetic_test_config["synthetic_settings"].items():
        file_path = os.path.join(output_dir, f"{ticker}.csv")
        df = pd.read_csv(file_path, index_col="Date", parse_dates=True)

        assert len(df) > 0, f"{ticker} CSV contains no rows."
        assert df.index.is_monotonic_increasing, f"{ticker} CSV index is not sorted."

        expected_columns = ["Open", "High", "Low", "Close", "Volume"]
        assert list(df.columns) == expected_columns, f"{ticker} CSV schema mismatch."
