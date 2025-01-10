import json
import pytest
from src.strategies.buy_and_hold import run_buy_and_hold


@pytest.fixture
def mock_config(tmp_path):
    """Create a temporary configuration file for testing."""
    config = {
        "symbol": "TEST",
        "start_date": "2023-01-01",
        "end_date": "2023-01-05",
        "capital_base": 100000,
    }
    config_path = tmp_path / "config.json"
    with open(config_path, "w") as f:
        json.dump(config, f)
    return str(config_path)


def test_buy_and_hold_json(mock_config, monkeypatch):
    """Test the strategy with mocked JSON configuration."""
    monkeypatch.setattr(
        "src.strategies.buy_and_hold.load_config", lambda _: mock_config
    )
    result = run_buy_and_hold()
    assert not result.empty, "Expected results, got an empty DataFrame."
    assert "price" in result.columns, "Price column missing in results."
