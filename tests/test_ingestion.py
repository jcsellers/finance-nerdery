
import os
import pandas as pd

import sys
print(sys.path)

from src.bundles.custom_bundle import generate_csv_from_db

#from tests.create_test_db import create_test_db
from .create_test_db import create_test_db


def test_csv_generation_with_deterministic_data(tmp_path):
    """Test CSV generation from deterministic test database."""
    test_db_path = "../data/output/deterministic_test_data.db"
    temp_csv_path = tmp_path / "temp_data.csv"

    # Generate CSV
    generate_csv_from_db(db_path=test_db_path, csv_path=str(temp_csv_path))

    # Validate file existence
    assert temp_csv_path.exists(), f"Expected CSV file {temp_csv_path} not found."

    # Validate CSV contents
    df = pd.read_csv(temp_csv_path)
    assert len(df) == 5  # 5 rows from the deterministic dataset
    assert df.iloc[0]['sid'] == 'TEST'
    assert df.iloc[0]['close'] == 105
