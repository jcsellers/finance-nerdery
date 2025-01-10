import pytest
from src.strategies.buy_and_hold import run_buy_and_hold

@pytest.fixture(scope="module", autouse=True)
def setup_bundle(tmp_path_factory):
    """Set up the database and ingest the bundle."""
    from tests.create_test_db import create_test_db
    import os

    # Create the test database
    db_path = tmp_path_factory.mktemp("data") / "deterministic_test_data.db"
    create_test_db(str(db_path))
    os.environ["DB_PATH"] = str(db_path)

    # Ingest the bundle
    os.system("python src/bundles/custom_bundle.py")


def test_buy_and_hold_json():
    """Test the strategy with JSON configuration."""
    result = run_buy_and_hold()
    assert not result.empty, "Expected results, got an empty DataFrame."
    assert "price" in result.columns, "Price column missing in results."
