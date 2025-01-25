# Finance Nerdery Project README

## Overview

Finance Nerdery is a comprehensive financial analysis pipeline designed to process financial data, calculate key metrics, compare strategies against benchmarks, and generate structured outputs. The pipeline is built with Python and leverages various libraries to ensure accurate and efficient analysis.

---

## Features

### **1. Data Pipeline Overview**

The data pipeline integrates financial data from two key sources:

- **Yahoo Finance**:
  - Provides historical OHLCV (Open, High, Low, Close, Volume) data for multiple tickers.
  - Data is fetched using the `YahooAcquisition` class, saved as both CSV and Parquet formats.

- **FRED (Federal Reserve Economic Data)**:
  - Supplies time series data for economic indicators (e.g., interest rates, credit spreads).
  - Data is fetched and transformed into OHLCV format via the `FredAcquisition` class.

- **Merging**:
  - Both datasets are aligned on NYSE trading days using the `DataMerger` class, ensuring a unified structure.
  - Missing values are handled via forward- and backward-filling.

- **Saving**:
  - Processed data is saved by the `DataSaver` class to both CSV and Parquet formats.
  - Validations ensure no duplicate columns or invalid structures.

---

### **2. Configuration Management**
The project uses a `config.toml` file to dynamically configure the pipeline, strategies, and outputs. Sample structure:

```toml
[sources.Yahoo_Finance]
tickers = ["SPY", "UPRO"]
start_date = "2020-01-01"
end_date = "2020-12-31"

[sources.FRED]
series_ids = ["BAMLH0A0HYM2"]

[output]
output_dir = "data/output"
formats = ["csv", "parquet"]
logs_dir = "logs"

[benchmarks]
[[benchmarks.strategies]]
strategy_name = "buy_and_hold"
ticker = "SPY"
```

---

### **3. Metrics and Trade Stats**

**Metrics** for each strategy:
- Sharpe Ratio
- Compound Annual Growth Rate (CAGR)
- Max Drawdown
- Total Return

**Trade Statistics**:
- Win Rate (%)
- Best/Worst Trade (%)
- Total Trades

These are extracted using `vectorbt`'s `Portfolio.performance()` and `Portfolio.stats()` methods.

---

## Prerequisites

Ensure you have the following installed:

- Python 3.7 or higher
- pip (Python package installer)

---

## Installation

### Clone the Repository
```bash
git clone https://github.com/yourusername/finance-nerdery.git
cd finance-nerdery
```

### Create a Virtual Environment (Optional but recommended)
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

---

## Configuration

Configure the pipeline using the `config.toml` file. Below is an example structure:

```toml
[sources.Yahoo_Finance]
tickers = ["SPY", "UPRO"]
start_date = "2020-01-01"
end_date = "2020-12-31"

[output]
output_dir = "data/output"
formats = ["csv", "parquet"]
logs_dir = "logs"

[benchmarks]
[[benchmarks.strategies]]
strategy_name = "buy_and_hold"
ticker = "SPY"
```

---

## Usage

### Prepare the Database
Ensure your financial data CSV files are placed in the `aligned` directory. Then, run the following script to import the data into the SQLite database:
```bash
python src/csv_to_sqlite_import.py
```

### Run the Analysis Pipeline
Execute the main script with the configuration file:
```bash
python src/main.py --config config/config.yaml
```
This will process the data, calculate metrics, compare with benchmarks, and generate the output JSON file as specified in the configuration.

---

## Directory Structure

Proposed structure for modular development:
```
src/
├── data_pipeline/
├── orchestrator.py
├── strategies/
└── tests/
```

---

## Output

The output JSON file will contain:
- **Metadata**: Information about the tickers analyzed, date range, and benchmarks.
- **Metrics**: Calculated financial metrics for each ticker.
- **Benchmark Comparison**: Metrics comparing each ticker against the specified benchmarks.

---

## Testing

Unit tests are provided to ensure the accuracy and reliability of the pipeline. To run the tests:

### Navigate to the Tests Directory
```bash
cd tests
```

### Execute Tests with pytest
```bash
pytest
```

The tests cover various aspects of the pipeline, including metric calculations, database interactions, and output generation. Mocking is used where appropriate to simulate external dependencies.

---

## Documentation

For detailed information on the pipeline's functionality, refer to the documentation.

---

## Additional Notes for FN_Developer GPTs

- Your role includes ensuring the pipeline is modular, extensible, and integrates seamlessly with new strategies.
- Prioritize robust logging and error handling.
- Always validate datasets before saving to ensure compatibility with downstream analysis.

---

