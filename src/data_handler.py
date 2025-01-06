import sqlite3
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataHandler:
    def __init__(self, db_path):
        self.db_path = db_path

    def connect_db(self):
        """
        Establish a connection to the SQLite database.

        Returns:
            sqlite3.Connection: SQLite database connection.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            logging.info("Connected to SQLite database.")
            return conn
        except sqlite3.Error as e:
            logging.error(f"SQLite connection error: {e}")
            raise

    def validate_database(self, required_tables):
        """
        Validate the presence of required tables and columns in the database.

        Parameters:
            required_tables (dict): A dictionary where keys are table names and values are
                                    lists of required column names.

        Raises:
            ValueError: If a required table or column is missing.
        """
        try:
            conn = self.connect_db()
            cursor = conn.cursor()
            for table, columns in required_tables.items():
                cursor.execute(f"PRAGMA table_info({table});")
                schema = cursor.fetchall()
                if not schema:
                    raise ValueError(f"Table '{table}' does not exist. Please create it.")

                existing_columns = {row[1]: row[2] for row in schema}
                for column, dtype in columns.items():
                    if column not in existing_columns:
                        raise ValueError(f"Table '{table}' is missing column '{column}'. Please add it.")
                    if dtype and dtype != existing_columns[column]:
                        raise ValueError(f"Column '{column}' in table '{table}' has incorrect type. "
                                         f"Expected '{dtype}', found '{existing_columns[column]}'.")

                logging.info(f"Table '{table}' validated successfully.")
        except Exception as e:
            logging.error(f"Database validation error: {e}")
            raise
        finally:
            conn.close()

    def fetch_data(self, tickers, start_date, end_date):
        """
        Fetch data from the database using parameterized queries.

        Parameters:
            tickers (list): List of tickers to fetch.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.

        Returns:
            DataFrame: The fetched data as a pandas DataFrame.

        Raises:
            ValueError: If fetching data fails or tickers are not found in the database.
        """
        conn = self.connect_db()
        try:
            # Validate tickers exist in the database
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT ticker FROM data;")
            available_tickers = {row[0] for row in cursor.fetchall()}
            missing_tickers = set(tickers) - available_tickers
            if missing_tickers:
                raise ValueError(f"Tickers {missing_tickers} not found in the database.")

            # Execute query
            query = """
                SELECT * FROM data 
                WHERE ticker IN ({}) 
                AND date BETWEEN ? AND ?;
            """.format(','.join(['?'] * len(tickers)))
            df = pd.read_sql_query(query, conn, params=tickers + [start_date, end_date])
            df['date'] = pd.to_datetime(df['date'])

            if df.empty:
                raise ValueError("No data retrieved for the given tickers and date range.")

            logging.info("Data successfully retrieved from SQLite.")
            return df
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            raise
        finally:
            conn.close()
