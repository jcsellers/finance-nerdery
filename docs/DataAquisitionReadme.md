# Data Acquisition Pipeline Documentation

## Overview
The Data Acquisition Pipeline is designed to fetch, process, and store financial data from various sources. It ensures data integrity, aligns dates to trading days, and outputs clean OHLCV data for analysis or backtesting.

### Supported Data Sources
1. **FRED**: Economic indicators like interest rates and spreads.
2. **Yahoo Finance**: Market data for stocks and ETFs.
3. **Synthetic Data**: Simulated data for testing edge cases.

---

## Configuration
The pipeline uses a JSON-like configuration to define tickers, date ranges, and settings for each data source.

### 1. FRED Configuration
```json
"FRED": {
    "api_key_env_var": "FRED_API_KEY"
},
"tickers": {
    "FRED": ["BAMLH0A0HYM2", "DGS10"]
},
"fred_settings": {
    "BAMLH0A0HYM2": {
        "start_date": "1997-01-01",
        "end_date": "2023-12-31",
        "alias": "OAS_Spread"
    },
    "DGS10": {
        "start_date": "1997-01-01",
        "end_date": "2023-12-31",
        "alias": "10Y_Treasury"
    }
}
```

### 2. Yahoo Finance Configuration
```json
"tickers": {
    "Yahoo Finance": ["AAPL", "MSFT"]
},
"aliases": {
    "Yahoo Finance": {
        "AAPL": "Apple",
        "MSFT": "Microsoft"
    }
},
"yahoo_finance_settings": {
    "AAPL": {
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "alias": "Apple"
    },
    "MSFT": {
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "alias": "Microsoft"
    }
}
```

### 3. Synthetic Data Configuration
```json
"tickers": {
    "Synthetic": ["TEST1"]
},
"synthetic_settings": {
    "TEST1": {
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "data_type": "linear",
        "start_value": 100,
        "growth_rate": 1.5
    }
}
```

### 4. Common Settings
```json
"settings": {
    "missing_data_handling": "interpolate",
    "output_dir": "output/"
}
```

---

## Data Types and Formats

### 1. Configuration Data Types
#### FRED Settings
- **`start_date`**: String, `"YYYY-MM-DD"` (e.g., `"1997-01-01"`).
- **`end_date`**: String, `"YYYY-MM-DD"` or `"current"`.
- **`alias`**: String, output file name (e.g., `"OAS_Spread"`).

#### Yahoo Finance Settings
- **`start_date`**: String, `"YYYY-MM-DD"` (e.g., `"2023-01-01"`).
- **`end_date`**: String, `"YYYY-MM-DD"` or `"current"`.
- **`alias`**: String, output file name (e.g., `"Apple"`).

#### Synthetic Data Settings
- **`start_date`**: String, `"YYYY-MM-DD"`.
- **`end_date`**: String, `"YYYY-MM-DD"`.
- **`data_type`**: String (`"linear"`, `"sinusoidal"`).
- **`start_value`**: Float, starting value.
- **`growth_rate`**: Float, growth per unit time.

#### Common Settings
- **`missing_data_handling`**: String (`"interpolate"`, `"forward_fill"`, `"flag"`).
- **`output_dir`**: String, directory for output CSVs (e.g., `"output/"`).

---

### 2. CSV Output Data Types
Each output CSV file adheres to the following schema:

| Column | Data Type | Format       | Example        |
|--------|-----------|--------------|----------------|
| Date   | datetime64[ns] | `YYYY-MM-DD` | `2023-01-01`   |
| Open   | float64   | Prices to 4 decimal places | `100.1234` |
| High   | float64   | Prices to 4 decimal places | `105.4567` |
| Low    | float64   | Prices to 4 decimal places | `99.9876`  |
| Close  | float64   | Prices to 4 decimal places | `104.3210` |
| Volume | int64     | Total traded volume        | `1234567`  |

---

## Validation Rules

1. **Date Index**:
   - Must be a `datetime64[ns]` index.
   - Aligns with NYSE trading days.

2. **Data Types**:
   - `Open`, `High`, `Low`, `Close`: `float64`.
   - `Volume`: `int64`.

3. **File Format**:
   - CSV files must be UTF-8 encoded.
   - Index labeled `"Date"`.

---

## Adding New Data Sources

### Steps:
1. **Create a Fetcher**:
   - Implement a new class following the fetcher interface.
   - Define a `fetch_data` method to retrieve data.

2. **Integrate into `DataPipeline`**:
   - Add a method like `process_new_source`.
   - Update `run()` to include the new method.

3. **Test**:
   - Write unit tests for the new fetcher.
   - Validate the output against the expected schema.

---

## Running the Pipeline

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set Up Environment
Add your FRED API key to `.env`:
```env
FRED_API_KEY=your_fred_api_key
```

### 3. Run the Pipeline
```bash
python src/data_pipeline.py
```

---

## Testing
Run the test suite to validate functionality:
```bash
pytest tests/
```

