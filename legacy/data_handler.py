import sqlite3
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataHandler:
    def __init__(self, db_path):
        self.db_path = db_path

    def connect_db(self):
        try:
            conn = sqlite3.connect(self.db_path)
            logging.info("Connected to SQLite database.")
            return conn
        except sqlite3.Error as e:
            logging.error(f"SQLite connection error: {e}")
            raise

    def fetch_data(self, tickers, start_date, end_date):
        conn = self.connect_db()
        try:
            query = """
                SELECT * FROM prices 
                WHERE ticker IN ({}) 
                AND timestamp BETWEEN ? AND ?;
            """.format(','.join(['?'] * len(tickers)))
            df = pd.read_sql_query(query, conn, params=tickers + [start_date, end_date])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            raise
        finally:
            conn.close()
