import os
import sqlite3


def load_environment_variables(env_path=".env"):
    """Load environment variables from a .env file."""
    from dotenv import load_dotenv

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
            os.makedirs(db_directory)

        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Execute schema creation scripts
        for table_name, schema in schemas.items():
            cursor.execute(schema)

        # Commit changes and close connection
        conn.commit()
        conn.close()
        print("Database setup successful.")
    except Exception as e:
        print(f"Error setting up database: {e}")


if __name__ == "__main__":
    # Load environment variables
    load_environment_variables()

    # Define database path and schemas
    db_path = os.getenv("DB_PATH")
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
