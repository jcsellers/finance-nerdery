import os
from unittest.mock import MagicMock

import pandas as pd
import pytest
from src.custom_ingest import custom_ingest


@pytest.fixture
def mock_writers():
    """Mock writers for asset_db, daily_bar, and adjustments."""
    return {
        "asset_db_writer": MagicMock(),
        "daily_bar_writer": MagicMock(),
        "adjustment_writer": MagicMock(),
    }


def test_csv_file_missing(mock_writers, monkeypatch):
    """Test error when the CSV file is missing."""
    monkeypatch.delenv("TRANSFORMED_CSV_PATH", raising=False)
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


def test_column_validation(mock_writers, tmp_path, monkeypatch):
    """Test error for missing columns."""
    mock_csv = tmp_path / "mock_data.csv"
    pd.DataFrame({"date": ["2025-01-01"], "open": [100]}).to_csv(mock_csv, index=False)
    monkeypatch.setenv("TRANSFORMED_CSV_PATH", str(mock_csv))

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


def test_metadata_writing(mock_writers, tmp_path, monkeypatch):
    """Test asset metadata writing."""
    mock_csv = tmp_path / "mock_data.csv"
    pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-02"],
            "open": [100.0, 102.0],
            "high": [105.0, 107.0],
            "low": [95.0, 97.0],
            "close": [102.0, 104.0],
            "volume": [1000, 1100],
        }
    ).to_csv(mock_csv, index=False)
    monkeypatch.setenv("TRANSFORMED_CSV_PATH", str(mock_csv))

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

    mock_writers["asset_db_writer"].write.assert_called_once()
    metadata = mock_writers["asset_db_writer"].write.call_args[0][0]
    assert not metadata.empty, "Asset metadata should not be empty."
    assert metadata["symbol"].iloc[0] == "SPY", "Symbol should be 'SPY'."


def test_corporate_actions_writing(mock_writers, tmp_path, monkeypatch):
    """Test corporate actions writing."""
    mock_csv = tmp_path / "mock_data.csv"
    pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-02"],
            "open": [100.0, 102.0],
            "high": [105.0, 107.0],
            "low": [95.0, 97.0],
            "close": [102.0, 104.0],
            "volume": [1000, 1100],
        }
    ).to_csv(mock_csv, index=False)
    monkeypatch.setenv("TRANSFORMED_CSV_PATH", str(mock_csv))

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

    mock_writers["adjustment_writer"].write.assert_called_once()
