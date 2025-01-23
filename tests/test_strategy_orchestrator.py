from pathlib import Path

import pytest

from src.strategy_orchestrator import StrategyOrchestrator


@pytest.fixture
def mock_config(tmp_path):
    """Mock configuration file."""
    config_path = tmp_path / "config.json"
    config_path.write_text(
        """
    {
        "parameters": {
            "target_asset": "spy_close"
        }
    }
    """
    )
    return config_path


@pytest.fixture
def mock_data(tmp_path):
    """Mock CSV data file with MultiIndex format."""
    data_path = tmp_path / "data.csv"
    data_path.write_text(
        """
    "('date', '')","('spy', 'close')"
    "2020-01-01",100
    "2021-01-01",120
    """
    )
    return data_path


def test_orchestrator_load_data(mock_config, mock_data):
    """Test data loading from a valid CSV file."""
    orchestrator = StrategyOrchestrator(config_path=mock_config, data_path=mock_data)
    data = orchestrator.load_data()
    assert "spy_close" in data.columns
    assert len(data) == 2


def test_orchestrator_load_data_invalid_path(mock_config):
    """Test error handling for an invalid data path."""
    invalid_data_path = Path("non_existent_file.csv")
    orchestrator = StrategyOrchestrator(
        config_path=mock_config, data_path=invalid_data_path
    )
    with pytest.raises(FileNotFoundError):
        orchestrator.load_data()


def test_orchestrator_run(mock_config, mock_data):
    """Test strategy execution and output."""
    orchestrator = StrategyOrchestrator(config_path=mock_config, data_path=mock_data)
    results = orchestrator.run()
    assert "CAGR [%]" in results
    assert results["CAGR [%]"] > 0


def test_orchestrator_target_asset_missing(mock_config, tmp_path):
    """Test handling when target asset is missing in data."""
    missing_data_path = tmp_path / "data.csv"
    missing_data_path.write_text(
        """
    "('date', '')","('spy', 'high')"
    "2020-01-01",100
    "2021-01-01",120
    """
    )
    orchestrator = StrategyOrchestrator(
        config_path=mock_config, data_path=missing_data_path
    )
    with pytest.raises(ValueError):
        orchestrator.run()
