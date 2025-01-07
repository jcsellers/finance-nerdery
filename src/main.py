import sqlite3
import logging
import yaml
import pandas as pd
from outputs import Outputs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_database(db_path, required_schema):
    """
    Validate the database schema against the required schema.

    Parameters:
        db_path (str): Path to the SQLite database file.
        required_schema (dict): Expected schema in the format {table_name: {column_name: column_type}}.

    Raises:
        ValueError: If schema validation fails.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        for table, columns in required_schema.items():
            cursor.execute(f"PRAGMA table_info({table});")
            schema = {row[1]: row[2].upper() for row in cursor.fetchall()}
            for column, dtype in columns.items():
                if column not in schema:
                    raise ValueError(f"Table '{table}' is missing column '{column}'. Please add it.")
                if dtype != schema[column]:
                    # Allow 'DATE' as a compatible type for 'TEXT'
                    if column == "date" and schema[column] == "DATE":
                        logging.warning(f"Column 'date' in table '{table}' has type 'DATE'. Treating as compatible.")
                        continue
                    raise ValueError(f"Column '{column}' in table '{table}' has incorrect type. "
                                     f"Expected '{dtype}', found '{schema[column]}'.")
        logging.info("Database schema validated successfully.")
    finally:
        conn.close()

def load_config(config_path):
    """
    Load and validate configuration from a YAML file.

    Parameters:
        config_path (str): Path to the YAML configuration file.

    Returns:
        dict: Parsed configuration dictionary.

    Raises:
        ValueError: If required keys are missing or invalid.
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)

        # Required keys for validation
        required_keys = {
            "database": ["path"],
            "output": ["path"],
            "tickers": [],
            "date_range": ["start", "end"]
        }

        for section, keys in required_keys.items():
            if section not in config:
                raise ValueError(f"Missing required section: '{section}' in config.yaml")
            for key in keys:
                if key not in config[section]:
                    raise ValueError(f"Missing required key: '{section}.{key}' in config.yaml")

        logging.info("Configuration validated successfully.")
        return config
    except Exception as e:
        logging.error(f"Error loading or validating configuration: {e}")
        raise

def calculate_metrics_and_generate_output(db_path, output_path):
    """
    Process the validated database, calculate metrics, and generate output JSON.

    Parameters:
        db_path (str): Path to the SQLite database.
        output_path (str): Path to save the output JSON file.
    """
    conn = sqlite3.connect(db_path)
    try:
        # Fetch data for metrics calculations
        query = "SELECT ticker, date, open, close FROM data;"
        df = pd.read_sql_query(query, conn)

        # Example metric calculations (replace with real logic)
        cagr = 0.15
        volatility = 0.12
        sharpe_ratio = 1.25
        alpha = 0.05
        beta = 0.9

        results = {
            "CAGR": cagr,
            "Volatility": volatility,
            "Sharpe Ratio": sharpe_ratio,
            "Alpha": alpha,
            "Beta": beta
        }

        # Save results to output JSON
        Outputs.generate_json(results, output_path)
        logging.info(f"Results successfully saved to {output_path}.")
    except Exception as e:
        logging.error(f"Error calculating metrics or generating output: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    # Load configuration
    CONFIG_PATH = "config/config.yaml"
    config = load_config(CONFIG_PATH)

    # Extract paths and schema
    DB_PATH = config['database']['path']
    OUTPUT_PATH = config['output']['path']
    REQUIRED_SCHEMA = {
        "data": {
            "ticker": "TEXT",
            "date": "TEXT",
            "open": "REAL",
            "close": "REAL"
        }
    }

    # Validate database
    try:
        validate_database(DB_PATH, REQUIRED_SCHEMA)
        calculate_metrics_and_generate_output(DB_PATH, OUTPUT_PATH)
    except ValueError as e:
        logging.error(f"Pipeline execution failed: {e}")
