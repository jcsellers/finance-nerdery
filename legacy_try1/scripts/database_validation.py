import logging
import sqlite3


def validate_database(db_path):
    """
    Validate the structure of the database.

    Parameters:
        db_path (str): Path to the SQLite database file.

    Returns:
        bool: True if validation is successful, raises an exception otherwise.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    expected_schema = {
        "prices": [
            {"name": "timestamp", "type": "TEXT"},
            {"name": "ticker", "type": "TEXT"},
            {"name": "open", "type": "REAL"},
            {"name": "high", "type": "REAL"},
            {"name": "low", "type": "REAL"},
            {"name": "close", "type": "REAL"},
            {"name": "volume", "type": "INTEGER"},
        ]
    }

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Validate tables
        for table, expected_columns in expected_schema.items():
            cursor.execute(f"PRAGMA table_info({table});")
            actual_columns = cursor.fetchall()
            if not actual_columns:
                raise ValueError(f"Table '{table}' does not exist in the database.")

            # Validate columns
            for expected_column in expected_columns:
                match = next(
                    (
                        col
                        for col in actual_columns
                        if col[1] == expected_column["name"]
                    ),
                    None,
                )
                if not match:
                    raise ValueError(
                        f"Missing column '{expected_column['name']}' in table '{table}'."
                    )
                if match[2].upper() != expected_column["type"].upper():
                    raise ValueError(
                        f"Column '{expected_column['name']}' in table '{table}' has incorrect type. "
                        f"Expected '{expected_column['type']}', found '{match[2]}'."
                    )

            logging.info(f"Table '{table}' validated successfully.")

    logging.info("Database validation successful.")
    return True


if __name__ == "__main__":
    db_path = "../data/aligned/trading_database.db"  # Update path if needed
    try:
        validate_database(db_path)
    except Exception as e:
        logging.error(f"Database validation failed: {e}")
