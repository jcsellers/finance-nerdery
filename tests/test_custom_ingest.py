import os
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.custom_ingest import custom_ingest


@pytest.fixture
def mock_writers():
    """Mock essential writers."""
    return {
        "asset_db_writer": MagicMock(),
        "daily_bar_writer": MagicMock(),
        "adjustment_writer": MagicMock(),
    }


@pytest.fixture
def synthetic_csv(tmp_path):
    """Generate a temporary CSV file for testing."""
    csv_path = tmp_path / "transformed_yahoo_data.csv"
    data = pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-02"],
            "open": [100.0, 102.0],
            "high": [105.0, 107.0],
            "low": [95.0, 97.0],
            "close": [102.0, 104.0],
            "volume": [1000, 1100],
        }
    )
    data.to_csv(csv_path, index=False)
    return csv_path


def test_csv_file_missing(mock_writers):
    """Test error when the CSV file is missing."""
    with pytest.raises(FileNotFoundError):
        custom_ingest(
            environ={},
            asset_db_writer=mock_writers["asset_db_writer"],
            minute_bar_writer=None,
            daily_bar_writer=mock_writers["daily_bar_writer"],
            adjustment_writer=mock_writers["adjustment_writer"],
            calendar=None,
            start_session=None,
            end_session=None,
            cache=None,
            show_progress=False,
            output_dir="output",
        )


def test_column_validation(mock_writers, synthetic_csv, monkeypatch):
    """Test missing required columns."""
    monkeypatch.setenv("TRANSFORMED_CSV_PATH", str(synthetic_csv))
    data = pd.read_csv(synthetic_csv).drop(columns=["open"])
    data.to_csv(synthetic_csv, index=False)

    with pytest.raises(ValueError, match="Missing required columns"):
        custom_ingest(
            environ={},
            asset_db_writer=mock_writers["asset_db_writer"],
            minute_bar_writer=None,
            daily_bar_writer=mock_writers["daily_bar_writer"],
            adjustment_writer=mock_writers["adjustment_writer"],
            calendar=None,
            start_session=None,
            end_session=None,
            cache=None,
            show_progress=False,
            output_dir="output",
        )


def test_metadata_writing(mock_writers, synthetic_csv, monkeypatch):
    """Test asset metadata writing."""
    monkeypatch.setenv("TRANSFORMED_CSV_PATH", str(synthetic_csv))

    custom_ingest(
        environ={},
        asset_db_writer=mock_writers["asset_db_writer"],
        minute_bar_writer=None,
        daily_bar_writer=mock_writers["daily_bar_writer"],
        adjustment_writer=mock_writers["adjustment_writer"],
        calendar=None,
        start_session=None,
        end_session=None,
        cache=None,
        show_progress=False,
        output_dir="output",
    )

    # Check metadata writing
    mock_writers["asset_db_writer"].write.assert_called_once()
    metadata = mock_writers["asset_db_writer"].write.call_args[0][0]
    assert not metadata.empty, "Asset metadata should not be empty."
    assert metadata["symbol"].iloc[0] == "SPY", "Symbol should be 'SPY'."
    assert metadata["start_date"].iloc[0] == pd.Timestamp(
        "2025-01-01"
    ), "Start date mismatch."
    assert metadata["end_date"].iloc[0] == pd.Timestamp(
        "2025-01-02"
    ), "End date mismatch."


def test_corporate_actions_writing(mock_writers, synthetic_csv, monkeypatch):
    """Test writing of splits and dividends."""
    monkeypatch.setenv("TRANSFORMED_CSV_PATH", str(synthetic_csv))

    custom_ingest(
        environ={},
        asset_db_writer=mock_writers["asset_db_writer"],
        minute_bar_writer=None,
        daily_bar_writer=mock_writers["daily_bar_writer"],
        adjustment_writer=mock_writers["adjustment_writer"],
        calendar=None,
        start_session=None,
        end_session=None,
        cache=None,
        show_progress=False,
        output_dir="output",
    )

    # Check splits and dividends writing
    mock_writers["adjustment_writer"].write.assert_called_once()
    splits = mock_writers["adjustment_writer"].write.call_args[1]["splits"]
    dividends = mock_writers["adjustment_writer"].write.call_args[1]["dividends"]

    assert splits["ratio"].iloc[0] == 0.5, "Split ratio should be 0.5."
    assert dividends["amount"].iloc[0] == 0.5, "Dividend amount should be 0.5."
