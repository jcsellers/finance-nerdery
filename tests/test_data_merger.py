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
        index=pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
    )

    # Create sample FRED data
    fred_data_dict = {
        "BAMLH0A0HYM2": pd.DataFrame(
            {
                "Value": [1.5, 1.6, 1.7],
            },
            index=pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
        )
    }

    # Merge datasets
    merged_data = DataMerger.merge_datasets(yahoo_data, fred_data_dict)

    # Assert merged_data contains both Yahoo and FRED columns
    assert "Value_BAMLH0A0HYM2" in merged_data.columns
