import sqlite3
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ALIGNED_DIR = "../aligned"
DB_PATH = "../aligned_data.db"

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
                    if column == "date" and schema[column] == "DATE":
                        logging.warning(f"Column 'date' in table '{table}' has type 'DATE'. Treating as compatible.")
                        continue
                    raise ValueError(f"Column '{column}' in table '{table}' has incorrect type. "
                                     f"Expected '{dtype}', found '{schema[column]}'.")
        logging.info("Database schema validated successfully.")
    finally:
        conn.close()

def setup_database(db_path):
    """
    Ensure the database has the required schema.

    Parameters:
        db_path (str): Path to the SQLite database file.
    """
    validate_database(db_path, {
        "data": {
            "ticker": "TEXT",
            "date": "TEXT",
            "open": "REAL",
            "close": "REAL"
        }
    })

def import_csv_to_sqlite(csv_path, db_path, table_name="data"):
    """
    Import data from a CSV file into an SQLite database.

    Parameters:
        csv_path (str): Path to the CSV file.
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the target table in SQLite.
    """
    conn = None
    try:
        df = pd.read_csv(csv_path)
        required_columns = ['ticker', 'date', 'open', 'close']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"Missing columns in CSV: {missing_columns}")
            return

        df.columns = df.columns.str.strip().str.lower()
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
        conn.commit()
        logging.info(f"Data from {csv_path} successfully imported into {table_name} table.")
    except Exception as e:
        logging.error(f"Error importing CSV to SQLite: {e}")
    finally:
        if conn:
            conn.close()

def process_all_csv_files(aligned_dir, db_path):
    """
    Process all CSV files in the specified directory.

    Parameters:
        aligned_dir (str): Path to the directory containing aligned CSV files.
        db_path (str): Path to the SQLite database file.
    """
    if not os.path.exists(aligned_dir):
        logging.error(f"Aligned directory {aligned_dir} does not exist.")
        return

    for file_name in os.listdir(aligned_dir):
        if file_name.endswith(".csv"):
            file_path = os.path.join(aligned_dir, file_name)
            logging.info(f"Processing file: {file_path}")
            import_csv_to_sqlite(file_path, db_path)

if __name__ == "__main__":
    setup_database(DB_PATH)
    process_all_csv_files(ALIGNED_DIR, DB_PATH)
