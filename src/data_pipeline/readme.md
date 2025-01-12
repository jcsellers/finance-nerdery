README

Overview

The Finance Nerdery Data Pipeline is a robust system designed to fetch, normalize, and save financial data from various sources. The pipeline integrates data from:

Yahoo Finance: Fetches historical financial data for specified tickers.

FRED: Retrieves economic data series.

Synthetic Data: Generates deterministic and customizable datasets for testing and analysis.

The pipeline outputs data in SQLite and CSV formats, ensuring compatibility with analysis tools like Zipline.

Installation

Prerequisites

Python 3.11 or higher

A virtual environment for dependency management

Setup

Clone the repository:

git clone <repository-url>
cd finance-nerdery

Create and activate a virtual environment:

python -m venv venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\activate   # Windows

Install dependencies:

pip install -r requirements.txt

Running the Pipeline

Default Configuration

Ensure the config.json file is located in the config/ directory relative to the project root.

Running the Script

Run the pipeline using the following command:

python -m src.data_pipeline.run_pipeline

Specifying a Custom Config Path

If the config.json file is located elsewhere, set the CONFIG_PATH environment variable:

export CONFIG_PATH=./path/to/config.json  # Linux/Mac
set CONFIG_PATH=./path/to/config.json     # Windows

Outputs

SQLite Database

Location: data/output/aligned_data.db

Tables:

yahoo_data: Contains historical financial data from Yahoo Finance.

fred_data: Includes normalized economic data from FRED.

synthetic_cash and synthetic_linear: Hold synthetic datasets.

CSV Files

Location: data/output/generated_data/

Files:

yahoo_data.csv: Yahoo Finance data.

fred_data.csv: FRED data.

synthetic_cash.csv: Synthetic cash dataset.

synthetic_linear.csv: Synthetic linear dataset.

Testing

Running Tests

The project uses pytest for testing. Run all tests with the following command:

pytest tests/

Areas Covered

Configuration validation

Data normalization

Handling missing values

SQLite data insertion and indexing

Feedback

If you encounter issues or have suggestions, please contact:

Name: [Your Name]Email: [Your Email]

