import os
from unittest.mock import patch

import pandas as pd
import pytest
from aquisition import FredAcquisition


@patch("acquisition.FredAcquisition.fetch_series")
def test_fred_acquisition(mock_fetch_series, tmp_path):
    # Mock the FRED API response
    mock_fetch_series.return_value = pd.DataFrame(
        {"Value": [1.5, 1.6, 1.7]},
        index=pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
    )

    api_key = "test_api_key"
    series_id = "BAMLH0A0HYM2"
    start_date = "2020-01-01"
    end_date = "2020-12-31"
    output_dir = tmp_path

    fred = FredAcquisition(api_key, cache_dir=output_dir)
    fred.fetch_and_save(
        series_id, start_date, end_date, f"{output_dir}/{series_id}.csv"
    )

    csv_path = f"{output_dir}/{series_id}.csv"
    assert os.path.exists(csv_path), "FRED CSV file was not created."
    df = pd.read_csv(csv_path, index_col="Date", parse_dates=True)
    assert "Value" in df.columns, "Missing 'Value' column in FRED CSV."
