# Scripts Folder

This folder contains the core scripts for the Finance Nerdery project. Each script serves a specific role in the data pipeline, from data fetching to database population.

## Scripts Overview

1. **`pipeline_orchestrator.py`**
   - Orchestrates the full data pipeline:
     1. Loads symbols from `../data/ticker_file.csv`.
     2. Fetches FRED economic data.
     3. Generates synthetic datasets.
     4. Fetches and cleans Yahoo Finance data.
     5. Aligns datasets to a common timeline.
     6. Populates the SQLite database.

2. **`fetch_fred_data.py`**
   - Fetches economic data from FRED using the API key in `.env`.
   - Maps FRED data to the schema required by the SQLite database.

3. **`fetch_and_clean_data.py`**
   - Fetches and cleans stock data from Yahoo Finance using `yfinance`.
   - Saves raw and cleaned datasets to the appropriate folders.

4. **`synthetic_dataset_generator.py`**
   - Generates synthetic datasets (`SYN_LINEAR` and `SYN_CASH`).
   - Maps data to the required schema and saves to `../data/aligned/`.

5. **`align_data.py`**
   - Aligns cleaned datasets to a common timeline, ensuring consistency across tickers and datasets.

6. **`create_sqlite_db.py`**
   - Creates and populates the SQLite database (`aligned_data.db`) with all processed datasets.

## Usage

### 1. Environment Setup
- Install dependencies:
  ```bash
  pip install -r ../requirements.txt
