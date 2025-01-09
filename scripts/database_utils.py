import sqlite3


def save_to_database(dataframe, database_path):
    """
    Saves a DataFrame to the SQLite database.
    """
    ticker = dataframe["ticker"].iloc[0]
    with sqlite3.connect(database_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM prices WHERE ticker = ?", (ticker,))
        conn.commit()
        dataframe.to_sql("prices", conn, if_exists="append", index=False)
