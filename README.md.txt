# Finance Nerdery

## Overview

Finance Nerdery is a comprehensive financial analysis pipeline designed to process financial data, calculate key metrics, compare strategies against benchmarks, and generate structured outputs. The pipeline is built with Python and leverages various libraries to ensure accurate and efficient analysis.

## Features

- **Data Import**: Import financial data from CSV files into an SQLite database.
- **Data Validation**: Ensure the database schema matches the expected structure.
- **Metrics Calculation**: Compute financial metrics including:
  - Compound Annual Growth Rate (CAGR)
  - Volatility
  - Sharpe Ratio
  - Sortino Ratio
  - Maximum Drawdown (MDD)
- **Benchmark Comparison**: Compare strategy performance against benchmarks, calculating Alpha and Beta.
- **JSON Output**: Generate structured JSON files containing metrics and metadata.
- **Unit Testing**: Validate the accuracy of metrics, database interactions, and outputs using `pytest`.

## Prerequisites

Ensure you have the following installed:

- Python 3.7 or higher
- `pip` (Python package installer)

## Installation

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/yourusername/finance-nerdery.git
   cd finance-nerdery
Create a Virtual Environment (Optional but recommended):

bash
Copy code
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
Install Dependencies:

bash
Copy code
pip install -r requirements.txt
Configuration
Configure the pipeline using the config.yaml file located in the config directory. Below is an example configuration:

yaml
Copy code
database:
  path: "aligned_data.db"
output:
  path: "results/output.json"
tickers:
  - "AAPL"
  - "MSFT"
date_range:
  start: "2020-01-01"
  end: "2023-01-01"
benchmarks:
  - "SPY"
database.path: Path to the SQLite database file.
output.path: Path where the JSON output will be saved.
tickers: List of stock tickers to analyze.
date_range: Start and end dates for the analysis.
benchmarks: List of benchmark tickers for comparison.
Usage
Prepare the Database:

Ensure your financial data CSV files are placed in the aligned directory. Then, run the following script to import the data into the SQLite database:

bash
Copy code
python src/csv_to_sqlite_import.py
Run the Analysis Pipeline:

Execute the main script with the configuration file:

bash
Copy code
python src/main.py --config config/config.yaml
This will process the data, calculate metrics, compare with benchmarks, and generate the output JSON file as specified in the configuration.

Output
The output JSON file will contain:

Metadata: Information about the tickers analyzed, date range, and benchmarks.
Metrics: Calculated financial metrics for each ticker.
Benchmark Comparison: Metrics comparing each ticker against the specified benchmarks.
Testing
Unit tests are provided to ensure the accuracy and reliability of the pipeline. To run the tests:

Navigate to the Tests Directory:

bash
Copy code
cd tests
Execute Tests with pytest:

bash
Copy code
pytest
The tests cover various aspects of the pipeline, including metric calculations, database interactions, and output generation. Mocking is used where appropriate to simulate external dependencies.

Documentation
For detailed information on the pipeline's functionality, refer to the documentation.

Contributing
Contributions are welcome! Please read the contributing guidelines for more information.

License
This project is licensed under the MIT License. See the LICENSE file for details.

