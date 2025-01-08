README: Database Creation and Synthetic Data Integration
Overview
This project automates the creation and population of a SQLite database with both real market data and synthetic datasets. The prices table includes a UNIQUE constraint to prevent duplicate rows, ensuring data integrity.

Project Structure
create_sqlite_db.py:

Deletes and rebuilds the database from scratch.
Fetches and aligns real market data.
Generates synthetic datasets (SYN_LINEAR and SYN_CASH).
Saves all data into the database.
synthetic_dataset_generator.py:

Focused solely on generating and saving synthetic datasets into the database.
data_generation.py:

Contains reusable functions for synthetic data generation:
generate_linear_trend: Produces a dataset with a linear price trend.
generate_cash_asset: Creates a consistent cash dataset.
database_utils.py:

Centralized helper for database interactions:
save_to_database: Appends new rows while ensuring data integrity with the UNIQUE constraint.
Key Features
Deduplication:

Prevents duplicate rows using the UNIQUE(ticker, timestamp) constraint in the prices table.
Synthetic Data:

Generates realistic datasets for testing strategies.
Clean Rebuild:

Ensures no legacy data issues by deleting the database before rebuilding it.
Usage
1. Setting Up the Environment
Install dependencies:
bash
Copy code
pip install -r requirements.txt
2. Creating and Populating the Database
Run the main script:

bash
Copy code
python create_sqlite_db.py
3. Generating Synthetic Datasets Only
To generate and save synthetic datasets:

bash
Copy code
python synthetic_dataset_generator.py
4. Testing
Run the test suite to ensure everything is working as expected:

bash
Copy code
pytest tests/test_synthetic_data.py -v
Database Schema
The prices table includes:

timestamp (TEXT): Date and time of the record.
ticker (TEXT): Identifier for the data source (e.g., "SYN_LINEAR").
open (REAL): Opening price.
high (REAL): Highest price.
low (REAL): Lowest price.
close (REAL): Closing price.
volume (REAL): Volume traded.
Constraints:

UNIQUE(ticker, timestamp): Prevents duplicate rows for the same ticker and timestamp.
Troubleshooting
Issue: Tests Failing for Schema Mismatch

Ensure the CREATE TABLE statement matches the expected column order.
Issue: Synthetic Cash Data Fails Validity Test

Verify that generate_cash_asset is used for SYN_CASH to maintain consistency across all price fields.
Future Enhancements
Add support for dynamic rebalancing strategies.
Visualize data trends with Matplotlib or Seaborn.
Integrate real-time data APIs (e.g., Yahoo Finance, FRED).
Would you like to add any specific sections or details?