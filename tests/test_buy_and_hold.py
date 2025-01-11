import pytest

from src.strategies.buy_and_hold import run_buy_and_hold


@pytest.fixture(scope="module", autouse=True)
def setup_bundle():
    """Ensure the custom bundle is ingested."""
    import os

    os.system("python src/bundles/custom_bundle.py")


def test_buy_and_hold_json():
    """Test the buy-and-hold strategy."""
    result = run_buy_and_hold()
    assert not result.empty, "Expected results, but got an empty DataFrame."
    assert "price" in result.columns, "Price column missing in results."
