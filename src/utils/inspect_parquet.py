import os

import pandas as pd

# Define the relative path to the Parquet file
file_path = os.path.join(
    os.path.dirname(__file__), "../../data/parquet_files/UPRO.parquet"
)

# Load the Parquet file
upro_data = pd.read_parquet(file_path)

# Inspect the data
print(upro_data.head())
