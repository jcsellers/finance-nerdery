# Data Acquisition for Finance Nerdery

This document outlines the processes, setup, and usage of the data acquisition system in the Finance Nerdery project. The system supports fetching, normalizing, validating, and saving financial data from Yahoo Finance and FRED, with outputs stored in MongoDB and Parquet files.

---

## Features

- Fetch historical stock and economic data:
  - **Yahoo Finance**: Stock prices and volumes.
  - **FRED**: Economic indicators such as OAS spreads and treasury yields.
- Normalize data to align with the NYSE calendar.
- Store data in MongoDB for querying and Parquet files for analysis.
- Automatically validate data, generating quality reports for review.

---

## Setup

### Prerequisites

- **Python**: Version 3.8 or higher.
- **MongoDB**: Installed locally or accessible remotely.
- **API Keys**:
  - **FRED**: Required to fetch economic data.

### Installation

1. Clone the repository:
   ```bash
   git clone <repo_url>
   cd finance-nerdery
   ```
2. Create a virtual environment and activate it:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   ```
3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Configuration

#### **`config/config.json`**
Update the configuration file to specify tickers, date ranges, and storage settings:
```json
{
    "date_ranges": {
        "start_date": "2020-01-02",
        "end_date": "current"
    },
    "tickers": {
        "yfinance": ["UPRO", "SSO", "SPY", "GLD", "TLT"],
        "FRED": ["BAMLH0A0HYM2", "DGS10", "VIXCLS"]
    },
    "aliases": {
        "FRED": {
            "BAMLH0A0HYM2": "OAS Spread",
            "DGS10": "10 Year Treasury Yield",
            "VIXCLS": "CBOE Volatility Index"
        }
    },
    "storage": {
        "MongoDB": {
            "connection_string": "mongodb://localhost:27017/",
            "database": "finance_nerdery",
            "collections": {
                "raw_data": "raw_financial_data",
                "error_logs": "ingestion_errors"
            }
        },
        "Parquet": {
            "directory": "data/parquet_files/"
        }
    }
}
```

#### **`.env` File**
Store your API keys in a `.env` file:
```plaintext
FRED_API_KEY=your_fred_api_key
```
Ensure the `.env` file is included in your `.gitignore` to prevent accidental exposure.

---

## Usage

### Run the Data Acquisition Pipeline
Execute the following command to run the entire pipeline:
```bash
python src/process_all_data.py
```

### Outputs
- **Parquet Files**: Saved in `data/parquet_files/`, with one file per ticker or FRED series.
- **Validation Reports**: Generated in `data/parquet_files/validation_reports/`.
- **MongoDB Storage**: Data is stored in the `raw_financial_data` collection.

---

## Project Structure

```
finance-nerdery/
├── config/                   # Configuration files
│   └── config.json           # Main configuration file
├── data/                     # Output data
│   ├── parquet_files/        # Parquet files and validation reports
├── src/                      # Source code
│   ├── fetchers/             # Fetching modules (FRED, Yahoo Finance)
│   ├── data_pipeline.py      # Core data pipeline logic
│   └── process_all_data.py   # Orchestrator for all processing
├── .env                      # Environment variables (API keys)
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
```

---

## Validation

For each processed ticker or series, validation reports are generated to ensure data quality:

- **Summary Statistics**: Includes count, mean, min, max, and standard deviation for numeric fields.
- **Missing Values**: Reports the count of missing values in each column.
- **Anomalies**: Flags zero or negative values in numeric fields.

Reports are saved in `data/parquet_files/validation_reports/`.

---

## Troubleshooting

- **MongoDB Not Saving Data**:
  - Ensure the MongoDB server is running (`mongod`).
  - Check the connection string in `config.json`.

- **FRED Data Fetch Errors**:
  - Verify your internet connection.
  - Confirm the `FRED_API_KEY` is correct and not expired.

- **Parquet Files Not Generated**:
  - Check for errors in the pipeline logs.
  - Ensure the output directory in `config.json` is writable.

---

## Next Steps

1. **Enhance Validation**:
   - Add checks for data consistency (e.g., expected ranges for stock prices).
   - Implement automated alerts for validation failures.

2. **Optimize Data Fetching**:
   - Add retries with exponential backoff for failed API calls.
   - Parallelize fetching for multiple tickers.

3. **Integrate Backtesting**:
   - Use the processed data in a backtesting framework like `vectorbt` or `Zipline`.

4. **Automate Execution**:
   - Schedule the pipeline to run periodically (e.g., with `cron` or GitHub Actions).

5. **Documentation Improvements**:
   - Add detailed examples for each function or module.
   - Include diagrams for the pipeline flow.

---

## Future Enhancements

- **Real-Time Data Processing**:
  - Incorporate live data feeds for intraday analysis.
- **Interactive Dashboards**:
  - Build a `Streamlit` or `Dash` app for visualizing data and running analyses interactively.
- **Expanded Data Sources**:
  - Add support for additional APIs or data providers.

---

This README will help you navigate the data acquisition portion of the Finance Nerdery project. Feel free to expand it as the project grows!

