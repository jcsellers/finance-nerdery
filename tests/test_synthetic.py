import os

import pandas as pd
import pytest

from DataPipeline import DataPipeline
from synthetic_data_generator import SyntheticDataGenerator
from synthetic_pipeline import SyntheticPipeline


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

        # Validate schema
        expected_columns = ["Open", "High", "Low", "Close", "Volume"]
        assert list(df.columns) == expected_columns, f"{ticker} CSV schema mismatch."

        # Validate content
        if settings["data_type"] == "linear":
            expected_values = [
                settings["start_value"] + i * settings["growth_rate"]
                for i in range(len(df))
            ]
            assert (
                df["Open"].values == expected_values
            ).all(), f"{ticker} Open column mismatch for linear data."
        elif settings["data_type"] == "cash":
            expected_values = [settings["start_value"]] * len(df)
            assert (
                df["Open"].values == expected_values
            ).all(), f"{ticker} Open column mismatch for cash data."

        # Validate consistency in OHLCV columns
        assert (df["High"] == df["Open"]).all(), f"{ticker} High column mismatch."
        assert (df["Low"] == df["Open"]).all(), f"{ticker} Low column mismatch."
        assert (df["Close"] == df["Open"]).all(), f"{ticker} Close column mismatch."
        assert (df["Volume"] == 10000).all(), f"{ticker} Volume column mismatch."
