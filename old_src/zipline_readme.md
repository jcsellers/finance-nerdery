# Zipline Pipeline Documentation

## Overview

The Zipline Pipeline transforms raw financial data into a Zipline-compatible format for backtesting and analysis. It supports both synthetic and database-driven workflows, ensuring flexibility and robustness.

### Key Features

- **Dynamic Column Mapping**: Automatically maps unconventional column names to standardized ones.
- **Versatile Data Handling**: Processes both synthetic data and data from SQLite databases.
- **Robust Error Handling**: Provides meaningful error messages and warnings for edge cases.
- **Integrated Logging**: Logs key steps for better traceability and debugging.

---

## Installation and Setup

### Prerequisites

- **Python Version**: Python 3.7 or higher.
- **Libraries**: Install required libraries:
  ```bash
  pip install pandas
  ```

### Environment Configuration

1. Set the `CONFIG_PATH` environment variable to point to the configuration file:

   ```bash
   export CONFIG_PATH=config/config.json
   ```

   On Windows:

   ```powershell
   $env:CONFIG_PATH = "config/config.json"
   ```

2. Ensure the SQLite database is accessible at the location specified in the configuration file.

---

## Usage

### Steps

1. Place the configuration file in the `config` directory.
2. Set the `CONFIG_PATH` environment variable if not using the default path.
3. Run the pipeline:
   ```bash
   python zipline_pipeline.py
   ```

---

## Configuration File

The configuration file defines the pipeline’s behavior and settings. Below is an example structure:

```json
{
  "date_ranges": {
    "start_date": "2020-01-01",
    "end_date": "current"
  },
  "storage": {
    "SQLite": "data/finance_data.db",
    "CSV": "data/csv_output/"
  },
  "tickers": {
    "Yahoo Finance": ["SPY", "SSO", "UPRO"]
  }
}
```

### Fields

- **`date_ranges`**:
  - `start_date`: Start of the date range.
  - `end_date`: End of the date range (use `current` for today).
- **`storage`**:
  - `SQLite`: Path to the SQLite database file.
  - `CSV`: Directory for saving transformed CSV files.
- **`tickers`**:
  - `Yahoo Finance`: List of tickers to process.

---

## Key Functions

### `load_config(config_path: str) -> dict`

- **Description**: Loads the configuration file.
- **Arguments**:
  - `config_path`: Path to the configuration file.
- **Returns**: Parsed configuration data.

### `generate_column_mapping(tickers: list) -> dict`

- **Description**: Generates a mapping from unconventional column names to standardized names.
- **Arguments**:
  - `tickers`: List of tickers to generate mappings for.
- **Returns**: Mapping dictionary.

### `transform_to_zipline(data: pd.DataFrame, config: dict, sid: int, column_mapping: dict) -> pd.DataFrame`

- **Description**: Transforms raw financial data into a Zipline-compatible DataFrame.
- **Arguments**:
  - `data`: Raw data as a Pandas DataFrame.
  - `config`: Configuration containing `start_date` and `end_date`.
  - `sid`: Security identifier.
  - `column_mapping`: Mapping from raw column names to standard names.
- **Returns**: Transformed DataFrame.

### `fetch_and_transform_data(...) -> pd.DataFrame`

- **Description**: Fetches data from a database or processes synthetic data, applies transformations, and saves as a CSV file.
- **Arguments**:
  - `database_path`: Path to the SQLite database file.
  - `table_name`: Name of the table to query.
  - `config`: Configuration settings.
  - `column_mapping`: Column mapping dictionary.
  - `output_dir`: Directory to save the output CSV.
  - `synthetic_data`: Synthetic data as a Pandas DataFrame (optional).
- **Returns**: Transformed DataFrame.

---

## Example Outputs

| date       | open  | high  | low  | close | volume | sid |
| ---------- | ----- | ----- | ---- | ----- | ------ | --- |
| 2025-01-01 | 100.0 | 105.0 | 95.0 | 102.0 | 1000   | 1   |
| 2025-01-02 | 102.0 | 107.0 | 97.0 | 104.0 | 1100   | 1   |

---

## Testing

### Steps

1. Set the `CONFIG_PATH` and `DB_PATH` environment variables:

   ```bash
   export CONFIG_PATH=config/config.json
   export DB_PATH=data/finance_data.db
   ```

   On Windows:

   ```powershell
   $env:CONFIG_PATH = "config/config.json"
   $env:DB_PATH = "data/finance_data.db"
   ```

2. Run the test suite:
   ```bash
   pytest tests --maxfail=1 --disable-warnings -v
   ```

---

## Error Handling and Logging

### Common Errors

- **Missing Columns**:

  - **Cause**: Raw data does not include required fields.
  - **Resolution**: Verify the raw data schema matches expectations.

- **Empty Dataset**:
  - **Cause**: No data matches the date range.
  - **Resolution**: Adjust the `start_date` and `end_date` in the configuration file.

### Logging

Logs are saved to the console to trace:

- Data fetching and schema validation.
- Transformation steps and outcomes.
- File saving locations and statuses.

---

## Contact

For questions or issues, contact the development team or refer to the project repository for further documentation.
