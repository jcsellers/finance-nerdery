import os

import pandas as pd
import pytest

from zipline_pipeline import (
    fetch_and_transform_data,
    generate_column_mapping,
    load_config,
    transform_to_zipline,
)


@pytest.fixture
def config():
    """Load configuration for testing."""
    config_path = os.getenv("CONFIG_PATH", "config/config.json")
    return load_config(config_path)


@pytest.fixture
def synthetic_data():
    """Generate synthetic data for unit tests."""
    return pd.DataFrame(
        {
            "('date', '')": ["2025-01-01", "2025-01-02"],
            "('spy', 'open')": [100.0, 102.0],
            "('spy', 'high')": [105.0, 107.0],
            "('spy', 'low')": [95.0, 97.0],
            "('spy', 'close')": [102.0, 104.0],
            "('spy', 'volume')": [1000, 1100],
        }
    )


def test_transform_to_zipline_with_empty_data(config):
    """Test transformation when no data matches the date range."""
    tickers = config["tickers"]["Yahoo Finance"]
    column_mapping = generate_column_mapping(tickers)

    # Empty dataset after filtering
    data = pd.DataFrame(
        {
            "('date', '')": ["2010-01-01"],
            "('spy', 'open')": [100.0],
            "('spy', 'high')": [105.0],
            "('spy', 'low')": [95.0],
            "('spy', 'close')": [102.0],
            "('spy', 'volume')": [1000],
        }
    )

    transformed_data = transform_to_zipline(
        data=data,
        config={"start_date": "2025-01-01", "end_date": "2025-12-31"},
        sid=1,
        column_mapping=column_mapping,
    )

    assert (
        transformed_data.empty
    ), "Expected an empty DataFrame when no data matches the date range."


def test_transform_to_zipline_missing_columns(config):
    """Test transformation when required columns are missing."""
    tickers = config["tickers"]["Yahoo Finance"]
    column_mapping = generate_column_mapping(tickers)

    # Dataset missing required columns
    data = pd.DataFrame(
        {
            "('date', '')": ["2025-01-01"],
            "('spy', 'close')": [102.0],
        }
    )

    with pytest.raises(ValueError, match="Missing required columns"):
        transform_to_zipline(
            data=data,
            config=config["date_ranges"],
            sid=1,
            column_mapping=column_mapping,
        )


def test_fetch_and_transform_with_synthetic_data(synthetic_data, config):
    """Test the pipeline with synthetic data."""
    tickers = config["tickers"]["Yahoo Finance"]
    column_mapping = generate_column_mapping(tickers)

    # Apply transformation using synthetic data
    transformed_data = fetch_and_transform_data(
        database_path=None,  # Not used for synthetic data
        table_name=None,  # Not used for synthetic data
        config=config,
        column_mapping=column_mapping,
        output_dir="output",  # Provide an output directory for synthetic data
        synthetic_data=synthetic_data,
    )

    # Validate transformed data
    assert not transformed_data.empty, "Transformed data is empty for synthetic data."
    assert "date" in transformed_data.columns, "Transformed data missing 'date' column."
    print("Synthetic data test passed.")


def test_output_file_creation(config, synthetic_data):
    """Test if output files are correctly created."""
    tickers = config["tickers"]["Yahoo Finance"]
    column_mapping = generate_column_mapping(tickers)

    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    fetch_and_transform_data(
        database_path=None,
        table_name=None,
        config=config,
        column_mapping=column_mapping,
        output_dir=output_dir,
        synthetic_data=synthetic_data,
    )

    output_file = f"{output_dir}/transformed_synthetic_data.csv"
    assert os.path.exists(output_file), f"Expected output file not found: {output_file}"
    print(f"Output file exists: {output_file}")
