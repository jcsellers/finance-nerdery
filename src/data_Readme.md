# Finance Nerdery Project

## Overview
This project combines **Yahoo Finance** and **FRED (Federal Reserve Economic Data)** to create a robust data pipeline for financial data acquisition, transformation, and merging. The system dynamically validates input dates against the NYSE calendar, fetches data from multiple sources, and saves the resulting merged dataset for downstream analysis.

## Key Features
1. **Yahoo Finance Integration**: Fetches historical market data for multiple tickers (e.g., `SPY`, `UPRO`, `TLT`).
2. **FRED Integration**: Downloads time series data (e.g., `BAMLH0A0HYM2`, `DGS10`) and transforms it into OHLCV format.
3. **Date Validation**: Ensures input dates align with valid NYSE trading days.
4. **Data Merging**: Combines datasets from Yahoo Finance and FRED, ensuring unique column names.
5. **Output Formats**: Saves data as CSV and Parquet files for flexibility in downstream applications.

---

## Design Overview

### **1. Data Acquisition**
#### **Yahoo Finance Acquisition**
- Fetches historical data for tickers using the `vectorbt` library.
- Handles missing data by forward-filling and backward-filling.
- Saves data to `data/yahoo_data.csv` and `data/yahoo_data.parquet`.

#### **FRED Acquisition**
- Fetches series data via the `fredapi` library.
- Handles missing data based on a configurable strategy (`interpolate`, `forward_fill`, or `flag`).
- Transforms data into OHLCV format for consistency with Yahoo Finance data.
- Saves data as individual CSV files (e.g., `data/BAMLH0A0HYM2.csv`).

---

### **2. Date Validation**
- Uses the `pandas_market_calendars` library to validate input dates against the NYSE calendar.
- Adjusts invalid dates (e.g., holidays or weekends) to the nearest valid trading day.
- Logs warnings for adjusted dates to inform users.

---

### **3. Data Merging**
- Aligns datasets from Yahoo Finance and FRED to NYSE trading days.
- Ensures unique column names by appending `_fred` to FRED column headers.
- Saves the merged dataset to `data/merged_data.csv`.

---

### **4. Configuration**
Configuration is managed via a `TOML` file:

#### **Example Configuration (`config.toml`)**
```toml
[sources.Yahoo_Finance]
tickers = ["SPY", "UPRO", "SSO", "TLT", "GLD", "^VIX", "^SPX"]
start_date = "2020-01-01"
end_date = "2020-12-31"

[sources.FRED]
api_key = "your_fred_api_key"
series_ids = ["BAMLH0A0HYM2", "DGS10"]

[date_ranges]
start_date = "2020-01-01"
end_date = "2020-12-31"

[output]
output_dir = "data"

[settings]
missing_data_handling = "interpolate"
```

---

## Key Components

### **1. YahooAcquisition**
Handles data fetching and saving for Yahoo Finance tickers.
- **Methods**:
  - `fetch_data`: Fetches data using `vectorbt` and aligns columns.
  - `save_data`: Saves data to CSV and Parquet formats.

### **2. FredAcquisition**
Handles data fetching, transformation, and saving for FRED series.
- **Methods**:
  - `fetch_series`: Fetches individual series with retry logic.
  - `handle_missing_data`: Processes missing data based on strategy.
  - `transform_to_ohlcv`: Converts data into OHLCV format.

### **3. DataMerger**
Merges Yahoo Finance and FRED datasets.
- **Methods**:
  - `merge_datasets`: Aligns and merges datasets, ensuring unique column names.

### **4. Orchestrator**
Manages the end-to-end pipeline.
- **Steps**:
  1. Validate dates.
  2. Fetch Yahoo Finance data.
  3. Fetch and transform FRED data.
  4. Merge datasets.
  5. Save the final output.

---

## Logging
- Logs important events and warnings, such as:
  - Adjusted dates.
  - Missing or invalid data.
  - Successful file saves.
- Logs are output to both the console and a `logs/` directory (if configured).

---

## Testing
### **1. Unit Tests**
- Validates individual components (e.g., `fetch_data`, `merge_datasets`).

### **2. Integration Tests**
- Verifies end-to-end functionality, including date validation, data fetching, and merging.

---

## Future Enhancements
1. **Error Handling**: Add more robust error recovery for network issues or missing data.
2. **Visualization**: Generate basic plots to visualize the merged dataset.
3. **Additional Data Sources**: Integrate other financial APIs (e.g., Quandl, Alpha Vantage).
4. **Parameterization**: Support dynamic date ranges and tickers from CLI arguments.

---

## Running the Pipeline
1. Ensure `.env` contains the `FRED_API_KEY`.
2. Prepare a valid `config.toml` file.
3. Run the pipeline:
   ```bash
   python src/data_orchestrator.py --config config.toml
   ```
4. Inspect the output in the `data/` directory.

---

## Contributors
Justin Sellers and Cygnus

---

## License
MIT License.
