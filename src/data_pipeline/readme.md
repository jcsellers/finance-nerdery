README
Overview
The Finance Nerdery Data Pipeline is a robust system designed to fetch, normalize, validate, and save financial data from multiple sources. It integrates data from:

Yahoo Finance: Fetches historical financial data for specified tickers.
FRED (Federal Reserve Economic Data): Retrieves economic data series using the FRED API.
Synthetic Data: Generates deterministic and customizable datasets for testing and analysis.
Key Features
Outputs data in SQLite and CSV formats for compatibility with tools like Zipline.
Includes row-by-row validation to ensure data consistency between SQLite and CSV outputs.
Modular pipeline with logging and error handling for seamless debugging and testing.
Installation
Prerequisites
Python 3.11 or higher
A virtual environment for dependency management
Setup
Clone the repository:

bash
Copy code
git clone <repository-url>
cd finance-nerdery
Create and activate a virtual environment:

bash
Copy code
python -m venv venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\activate   # Windows
Install dependencies:

bash
Copy code
pip install -r requirements.txt
Running the Pipeline
Default Configuration
Ensure the config.json file is located in the config/ directory relative to the project root.
The .env file in the project root should include:
plaintext
Copy code
FRED_API_KEY=your_actual_fred_api_key
Running the Script
Run the pipeline using the following command:

bash
Copy code
python -m src.data_pipeline.run_pipeline
Specifying a Custom Config Path
If the config.json file is located elsewhere, set the CONFIG_PATH environment variable:

bash
Copy code
export CONFIG_PATH=./path/to/config.json  # Linux/Mac
set CONFIG_PATH=./path/to/config.json     # Windows
Outputs
SQLite Database
Location: data/finance_data.db
Tables:
yahoo_data: Historical financial data from Yahoo Finance.
fred_data: Normalized economic data from FRED.
synthetic_cash and synthetic_linear: Synthetic datasets.
CSV Files
Location: data/csv_files/
Files:
yahoo_data.csv: Yahoo Finance data.
fred_data.csv: FRED data.
synthetic_cash.csv: Synthetic cash dataset.
synthetic_linear.csv: Synthetic linear dataset.
Testing
Running Tests
The project uses pytest for testing. Run all tests with the following command:

bash
Copy code
pytest tests/
Areas Covered
Configuration validation
Data fetching and normalization
Handling missing values
Row-by-row validation
SQLite data insertion and indexing
Feedback
If you encounter issues or have suggestions, please contact:

Name: Justin Sellers
Email: [Your Email Here]

