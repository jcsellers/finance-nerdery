from unittest.mock import MagicMock, patch

import pytest
from run_pipeline import main


@pytest.fixture
def mock_config():
    """Mock configuration for pipelines."""
    return {
        "storage": {"SQLite": "test_db.sqlite"},
        "tickers": {
            "Alpha Vantage": ["AAPL", "GOOG"],
            "FRED": ["DGS10", "VIXCLS"],
        },
        "aliases": {
            "FRED": {
                "DGS10": "10 Year Treasury Yield",
                "VIXCLS": "CBOE Volatility Index",
            }
        },
        "date_ranges": {"start_date": "2020-01-01", "end_date": "2022-12-31"},
        "Synthetic Data": {
            "syn_cash": {"start_value": 1.0},
            "syn_linear": {"start_value": 1.0, "growth_rate": 0.01},
        },
    }


@patch("run_pipeline.AlphaVantagePipeline")
@patch("run_pipeline.FredPipeline")
@patch("run_pipeline.SyntheticPipeline")
@patch("run_pipeline.NasdaqPipeline")
@patch("run_pipeline.YahooPipeline")
@patch("run_pipeline.save_to_db")
@patch("run_pipeline.ensure_table_schema")
def test_run_pipeline(
    mock_ensure_schema,
    mock_save_to_db,
    mock_yahoo_pipeline,
    mock_nasdaq_pipeline,
    mock_synthetic_pipeline,
    mock_fred_pipeline,
    mock_alpha_vantage_pipeline,
    mock_config,
):
    """Test the run_pipeline.py main function."""
    # Mock pipelines
    mock_alpha_instance = MagicMock()
    mock_alpha_instance.run.return_value = MagicMock(empty=False)
    mock_alpha_vantage_pipeline.return_value = mock_alpha_instance

    mock_fred_instance = MagicMock()
    mock_fred_instance.run.return_value = MagicMock(empty=False)
    mock_fred_pipeline.return_value = mock_fred_instance

    mock_synthetic_instance = MagicMock()
    mock_synthetic_instance.run.return_value = MagicMock(empty=False)
    mock_synthetic_pipeline.return_value = mock_synthetic_instance

    mock_nasdaq_instance = MagicMock()
    mock_nasdaq_instance.run.return_value = MagicMock(empty=False)
    mock_nasdaq_pipeline.return_value = mock_nasdaq_instance

    mock_yahoo_instance = MagicMock()
    mock_yahoo_instance.run.return_value = MagicMock(empty=False)
    mock_yahoo_pipeline.return_value = mock_yahoo_instance

    # Mock the ensure_table_schema and save_to_db to avoid actual DB writes
    mock_ensure_schema.return_value = None
    mock_save_to_db.return_value = None

    # Run the pipeline
    with patch("run_pipeline.CONFIG_PATH", "mock_config.json"):
        main()

    # Verify pipeline execution
    mock_alpha_vantage_pipeline.assert_called_once()
    mock_fred_pipeline.assert_called_once()
    mock_synthetic_pipeline.assert_called_once()
    mock_nasdaq_pipeline.assert_called_once()
    mock_yahoo_pipeline.assert_called_once()

    # Verify data-saving
    assert mock_save_to_db.call_count == 5
