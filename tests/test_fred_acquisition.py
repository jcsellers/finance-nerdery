import os
from unittest.mock import patch

import pandas as pd

from acquisition import FredAcquisition


@patch("acquisition.FredAcquisition.fetch_series")
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

    fred = FredAcquisition(api_key, cache_dir=tmp_path)

    # Fetch and transform the series
    fred_data = fred.fetch_series(series_id, start_date, end_date)
    ohlcv_data = fred.transform_to_ohlcv(fred_data)

    # Assert the transformed data has the correct columns
    assert all(
        col in ohlcv_data.columns for col in ["Open", "High", "Low", "Close", "Volume"]
    )
