import pandas as pd
import pytest

from merging import DataMerger


def test_data_merger_with_trading_day_alignment():
    # Create sample Yahoo Finance data
    yahoo_data = pd.DataFrame(
        {
            "SPY": [300, 301, 302],
            "UPRO": [100, 101, 102],
        },
        index=pd.to_datetime(
            ["2020-01-01", "2020-01-02", "2020-01-03"]
        ),  # Includes a holiday
    )

    # Create sample FRED data
    fred_data = pd.DataFrame(
        {
            "BAMLH0A0HYM2": [1.5, 1.6, 1.7],
        },
        index=pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
    )

    # Merge datasets
    merged_data = DataMerger.merge_datasets(
        yahoo_data, fred_data, "2020-01-01", "2020-01-03"
    )

    # Validate merged dataset
    expected_dates = ["2020-01-02", "2020-01-03"]  # Only valid NYSE trading days
    assert (
        list(merged_data.index.strftime("%Y-%m-%d")) == expected_dates
    ), "Merged data is not aligned to trading days."
    assert "SPY" in merged_data.columns, "SPY column missing in merged data."
    assert "BAMLH0A0HYM2" in merged_data.columns, "FRED column missing in merged data."
