import argparse
import os
import sqlite3

from dotenv import load_dotenv


def load_environment_variables(env_path=".env"):
    """Load environment variables from a .env file."""
    load_dotenv(env_path)


def setup_database(db_path, schemas):
    """
    Sets up the SQLite database schema.
    :param db_path: Path to the SQLite database.
    :param schemas: Dictionary of table names and their corresponding SQL schema.
    """
    try:
        # Ensure the database directory exists
        db_directory = os.path.dirname(db_path)
        if not os.path.exists(db_directory):
            try:
                os.makedirs(db_directory)
            except OSError as e:
                raise OSError(f"Failed to create directory {db_directory}: {e}")

        # Connect to SQLite database
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Execute schema creation scripts
            for table_name, schema in schemas.items():
                try:
                    cursor.execute(schema)
                    print(f"Table {table_name} created or already exists.")
                except sqlite3.Error as e:
                    print(f"Error creating table {table_name}: {e}")

            conn.commit()
        print("Database setup successful.")
    except Exception as e:
        print(f"Error setting up database: {e}")


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Setup SQLite database with predefined schemas."
    )
    parser.add_argument(
        "--env_path", type=str, default=".env", help="Path to the .env file"
    )
    parser.add_argument(
        "--db_path", type=str, help="Path to the SQLite database", default=None
    )
    args = parser.parse_args()

    # Load environment variables
    load_environment_variables(args.env_path)

    # Define database path and schemas
    db_path = args.db_path or os.getenv("DB_PATH")
    if not db_path:
        raise ValueError(
            "DB_PATH environment variable is not set and no --db_path argument provided."
        )

    schemas = {
        "historical_data": """
            CREATE TABLE IF NOT EXISTS historical_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                adjusted_close REAL,
                volume INTEGER,
                source TEXT DEFAULT 'yfinance',
                is_filled BOOLEAN DEFAULT 0
            );
        """,
        "economic_data": """
            CREATE TABLE IF NOT EXISTS economic_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                alias TEXT,
                timestamp DATETIME NOT NULL,
                value REAL,
                source TEXT DEFAULT 'FRED',
                is_filled BOOLEAN DEFAULT 0
            );
        """,
        "synthetic_data": """
            CREATE TABLE IF NOT EXISTS synthetic_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_name TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                value REAL,
                metadata TEXT
            );
        """,
    }

    # Setup the database
    setup_database(db_path, schemas)
