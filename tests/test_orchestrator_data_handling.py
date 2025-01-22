from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.strategy_orchestrator import StrategyOrchestrator


@patch("fred_data_reader.FredDataReader")
def test_load_data_fred(mock_fred_reader):
    # Mock the FredDataReader behavior
    mock_instance = mock_fred_reader.return_value
    mock_instance.get_ticker_data.return_value = pd.DataFrame(
        {"value": [3.5, 3.6, 3.7]},
        index=pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
    )

    # Mock both config_path and data_path
    with patch.object(Path, "exists", return_value=True), patch(
        "builtins.open", new_callable=MagicMock
    ) as mock_open:
        # Mock the configuration file
        mock_open.return_value.__enter__.return_value.read.return_value = '{"strategy_name": "buy_and_hold", "parameters": {"target_asset": "fred_ticker"}}'

        orchestrator = StrategyOrchestrator(
            config_path="config/fred_strategy.json",
            data_path=str(Path("mock_data/fred_data.csv")),
        )
        result = orchestrator.load_data()

    # Assert that the mocked FredDataReader methods were called
    mock_instance.load_data.assert_called_once()
    mock_instance.get_ticker_data.assert_called_once()
    assert not result.empty


@patch("yahoo_data_reader.YahooDataReader")
def test_load_data_yahoo(mock_yahoo_reader):
    # Mock the YahooDataReader behavior
    mock_instance = mock_yahoo_reader.return_value
    mock_instance.get_ticker_data.return_value = pd.DataFrame(
        {"close": [100, 101, 102]},
        index=pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
    )

    # Mock both config_path and data_path
    with patch.object(Path, "exists", return_value=True), patch(
        "builtins.open", new_callable=MagicMock
    ) as mock_open:
        # Mock the configuration file
        mock_open.return_value.__enter__.return_value.read.return_value = '{"strategy_name": "buy_and_hold", "parameters": {"target_asset": "yahoo_ticker"}}'

        orchestrator = StrategyOrchestrator(
            config_path="config/yahoo_strategy.json",
            data_path=str(Path("mock_data/yahoo_data.csv")),
        )
        result = orchestrator.load_data()

    # Assert that the mocked YahooDataReader methods were called
    mock_instance.load_data.assert_called_once()
    mock_instance.get_ticker_data.assert_called_once()
    assert not result.empty


@patch("pandas.read_csv")
def test_load_data_fallback_csv(mock_read_csv):
    # Mock pandas.read_csv behavior
    mock_read_csv.return_value = pd.DataFrame(
        {
            "date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "close": [100, 101, 102],
        }
    ).set_index("date")

    # Mock both config_path and data_path
    with patch.object(Path, "exists", return_value=True), patch(
        "builtins.open", new_callable=MagicMock
    ) as mock_open:
        # Mock the configuration file
        mock_open.return_value.__enter__.return_value.read.return_value = '{"strategy_name": "buy_and_hold", "parameters": {"target_asset": "generic_ticker"}}'

        orchestrator = StrategyOrchestrator(
            config_path="config/generic_strategy.json",
            data_path=str(Path("mock_data/generic_data.csv")),
        )
        result = orchestrator.load_data()

    # Convert Path to string to avoid WindowsPath mismatch
    mock_read_csv.assert_called_once_with(
        "mock_data\\generic_data.csv", parse_dates=["date"], index_col="date"
    )
    assert not result.empty
