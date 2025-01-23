import argparse
import json
import logging
from pathlib import Path

import pandas as pd


# YahooDataReader class with fixes
class YahooDataReader:
    def __init__(self, file_path):
        self.file_path = Path(file_path)
        self.data = None

    def load_data(self):
        """Load the Yahoo Finance data."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"Data file not found at {self.file_path}")
        logging.info(f"Loading data from {self.file_path}")
        self.data = pd.read_csv(
            self.file_path, header=[0, 1]
        )  # Load hierarchical columns
        self.flatten_columns()

    def flatten_columns(self):
        """Flatten MultiIndex columns for easier access."""
        if isinstance(self.data.columns, pd.MultiIndex):
            self.data.columns = [
                f"{level1.lower()}_{level2.lower()}" if level2 else level1.lower()
                for level1, level2 in self.data.columns
            ]
            logging.info(f"Flattened columns: {self.data.columns}")

    def get_symbol_data(self, symbol):
        """Extract data for a specific symbol."""
        self.load_data()
        logging.info(f"Filtering data for symbol: {symbol}")
        try:
            # Normalize the symbol for matching
            normalized_symbol = symbol.lower()

            # Strip suffixes from column names and create a mapping
            stripped_columns = {col.split("_")[0]: col for col in self.data.columns}

            # Attempt an exact match first
            if normalized_symbol in stripped_columns:
                matched_column = stripped_columns[normalized_symbol]
                logging.info(f"Exact match found for {symbol}: {matched_column}")
                return self.data[[matched_column]]

            # Debug possible matches
            logging.error(
                f"Available stripped columns: {list(stripped_columns.keys())}"
            )
            raise KeyError(f"No matching columns found for symbol: {symbol}")
        except KeyError as e:
            logging.error(f"Error extracting data for symbol {symbol}: {e}")
            raise ValueError(f"No data found for symbol: {symbol}")
