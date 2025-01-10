import os
import pytest
from zipline.api import symbol
from zipline.utils.run_algo import run_algorithm
from datetime import datetime
from strategies.buy_and_hold import run_buy_and_hold

# Ensure the temp CSV is generated and ingested before tests
@pytest.fixture(scope="session", autouse=True)
def setup_custom_bundle():
    TEMP_CSV_PATH = "data/output/zipline_temp_data.csv"
    os.makedirs(os.path.dirname(TEMP_CSV_PATH), exist_ok=True)

    # Generate CSV if it doesn't exist
    if not os.path.exists(TEMP_CSV_PATH):
        with open(TEMP_CSV_PATH, "w") as f:
            f.write(
                "sid,date,open,high,low,close,volume\n"
                "TEST,2023-01-01,100,110,90,105,1000\n"
                "TEST,2023-01-02,105,115,95,110,1200\n"
                "TEST,2023-01-03,110,120,100,115,1300\n"
                "TEST,2023-01-04,115,125,105,120,1400\n"
                "TEST,2023-01-05,120,130,110,125,1500\n"
            )

    # Ingest the bundle
    os.system("zipline ingest -b custom_csv")


# Example test
def test_buy_and_hold_json():
    """Test the strategy with JSON configuration."""
    result = run_buy_and_hold()
    assert result.portfolio_value[-1] > result.portfolio_value[0]
