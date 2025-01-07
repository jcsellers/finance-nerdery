import sqlite3
import logging
import yaml

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

if __name__ == "__main__":
    # Load configuration
    CONFIG_PATH = "config/config.yaml"
    config = load_config(CONFIG_PATH)

    # Extract paths and schema
    DB_PATH = config['database']['path']
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
    except ValueError as e:
        logging.error(f"Database validation error: {e}")
        raise
