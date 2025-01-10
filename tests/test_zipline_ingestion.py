import os
import pytest
from zipline.data import bundles


@pytest.fixture
def setup_bundle():
    """Ensure the bundle is ingested before testing."""
    os.system("python src/bundles/custom_bundle.py")


def test_zipline_ingestion(setup_bundle):
    """Test that the custom bundle is ingested correctly."""
    bundle_name = "custom_csv"
    custom_bundle = bundles.load(bundle_name)

    # Validate asset metadata
    assets = custom_bundle.asset_finder.retrieve_all()
    assert len(assets) == 1, "Expected 1 asset in the bundle."
    assert assets[0].symbol == "TEST", "Expected asset symbol 'TEST'."

    # Validate data integrity
    calendar = custom_bundle.trading_calendar.sessions_in_range(
        custom_bundle.equity_daily_bar_reader.first_trading_day,
        custom_bundle.equity_daily_bar_reader.last_trading_day,
    )
    reader = custom_bundle.equity_daily_bar_reader
    data = reader.load_raw_arrays(
        ["open", "high", "low", "close", "volume"], calendar, [assets[0].sid]
    )
    assert data is not None, "No data loaded from the bundle."
