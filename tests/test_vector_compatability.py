import os

import pandas as pd
import pytest
import toml
import vectorbt as vbt


@pytest.fixture
def config():
    """Fixture to load the TOML configuration."""
    config_path = "config.toml"  # Replace this with the actual path if needed
    return toml.load(config_path)


@pytest.fixture
def merged_data(config):
    """Fixture to load the merged dataset for testing."""
    output_dir = config["output"]["output_dir"]
    file_path = os.path.join(output_dir, "merged_data.csv")
    assert os.path.exists(file_path), f"File not found: {file_path}"
    data = pd.read_csv(file_path, index_col=0, parse_dates=True)
    return data


def test_vectorbt_compatibility(merged_data):
    """Test to verify compatibility of merged data with vectorbt."""
    # Ensure the data has a valid DateTimeIndex
    assert isinstance(
        merged_data.index, pd.DatetimeIndex
    ), "Index must be a DateTimeIndex"

    # Ensure all columns are numeric or can be converted to numeric
    numeric_data = merged_data.apply(pd.to_numeric, errors="coerce")
    assert (
        not numeric_data.isnull().all().any()
    ), "All columns must be numeric or convertible to numeric"

    # Test with vectorbt's Backtesting module
    try:
        # Create a sample price series from one of the columns (e.g., 'Close_SPY')
        price_series = merged_data["Close_SPY"]
        portfolio = vbt.Portfolio.from_holding(price_series)

        # Verify portfolio object creation
        assert (
            portfolio.total_profit() is not None
        ), "Portfolio should calculate total profit successfully"
    except Exception as e:
        pytest.fail(f"vectorbt compatibility test failed: {e}")
