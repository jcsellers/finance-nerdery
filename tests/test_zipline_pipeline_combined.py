import os
import sqlite3

import pandas as pd
import pytest

from zipline_pipeline import (
    fetch_and_transform_data,
    generate_column_mapping,
    load_config,
)


@pytest.fixture
def config():
    """Load configuration for testing."""
    config_path = os.getenv(
        "CONFIG_PATH", "config.json"
    )  # Use environment variable or default
    return load_config(config_path)


@pytest.fixture
def real_database_connection():
    """Fixture for connecting to the real database."""
    db_path = os.getenv(
        "DB_PATH", "data/finance_data.db"
    )  # Use environment variable or default
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()


@pytest.fixture
def synthetic_data():
    """Generate synthetic data for unit tests."""
    data = pd.DataFrame(
        {
            "('date', '')": ["2025-01-01", "2025-01-02"],
            "('spy', 'open')": [100.0, 102.0],
            "('spy', 'high')": [105.0, 107.0],
            "('spy', 'low')": [95.0, 97.0],
            "('spy', 'close')": [102.0, 104.0],
            "('spy', 'volume')": [1000, 1100],
        }
    )
    return data


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
    print("Synthetic data test passed.")


def test_fetch_and_transform_with_real_data(real_database_connection, config):
    """Test the pipeline against the real database."""
    tickers = config["tickers"]["Yahoo Finance"]
    column_mapping = generate_column_mapping(tickers)

    # Run the pipeline
    fetch_and_transform_data(
        database_path=os.getenv(
            "DB_PATH", "data/finance_data.db"
        ),  # Use environment variable or default
        table_name="yahoo_data",
        config=config,
        column_mapping=column_mapping,
        output_dir="output/",  # Update path as needed
    )

    # Validate the output
    output_file = "output/transformed_yahoo_data.csv"  # Update path as needed
    transformed_data = pd.read_csv(output_file)
    assert not transformed_data.empty, "Transformed data is empty for real database."
    print("Real database test passed.")
