import logging
import sqlite3
import time
from datetime import datetime

import pandas as pd
import yfinance as yf

from src.utils import fill_missing_market_days


def fetch_yfinance_data(tickers, start_date, end_date="current", db_path=None):
    """
    Fetches historical data for a list of tickers using yfinance and stores it in SQLite.

    :param tickers: List of ticker symbols.
    :param start_date: Start date for historical data.
    :param end_date: End date for historical data. Defaults to "current".
    :param db_path: Path to the SQLite database.
    """
    # Set up logging
    logger = logging.getLogger("yfinance_fetcher")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    if end_date == "current":
        end_date = datetime.now().strftime("%Y-%m-%d")

    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for ticker in tickers:
            logger.info(f"Fetching data for {ticker}")
            try:
                # Fetch data from yfinance
                data = yf.download(ticker, start=start_date, end=end_date)
                if data.empty:
                    logger.warning(f"No data found for {ticker}")
                    continue

                # Normalize and handle missing market days using NYSE calendar
                data.reset_index(inplace=True)
                logger.debug(f"Before filling missing days:\n{data.head()}")
                data = fill_missing_market_days(data, start_date, end_date)
                logger.debug(f"After filling missing days:\n{data.head()}")
                data["ticker"] = ticker
                data["source"] = "yfinance"
                data["is_filled"] = 0

                # Deduplicate and insert data into SQLite
                for _, row in data.iterrows():
                    logger.debug(f"Processing row: {row}")

                    # Extract scalar timestamp
                    try:
                        timestamp = row["Date"]
                        if isinstance(timestamp, pd.Timestamp):
                            timestamp = timestamp.to_pydatetime()
                        elif isinstance(timestamp, str):
                            timestamp = pd.to_datetime(timestamp)
                        else:
                            logger.error(f"Invalid timestamp format: {row['Date']}")
                            continue  # Skip invalid rows
                    except Exception as e:
                        logger.error(f"Error converting timestamp: {e}")
                        continue

                    open_price = row.get("Open", None)
                    high_price = row.get("High", None)
                    low_price = row.get("Low", None)
                    close_price = row.get("Close", None)
                    adj_close = row.get("Adj Close", close_price)
                    volume = row.get("Volume", None)

                    if pd.isna(timestamp) or pd.isna(close_price):
                        logger.warning(
                            f"Skipping row with missing data for {ticker}: {row}"
                        )
                        continue

                    # Check if the record already exists in the database
                    cursor.execute(
                        """
                        SELECT COUNT(*) FROM historical_data
                        WHERE ticker = ? AND timestamp = ? AND source = ?;
                    """,
                        (ticker, timestamp, "yfinance"),
                    )
                    exists = cursor.fetchone()[0]

                    if exists == 0:
                        cursor.execute(
                            """
                            INSERT INTO historical_data (ticker, timestamp, open, high, low, close, adjusted_close, volume, source, is_filled)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                            (
                                ticker,
                                timestamp,
                                open_price,
                                high_price,
                                low_price,
                                close_price,
                                adj_close,
                                volume,
                                "yfinance",
                                0,
                            ),
                        )

                conn.commit()
                logger.info(f"Successfully stored data for {ticker}")
                time.sleep(1)  # Rate limiting to avoid API limits

            except Exception as e:
                logger.error(f"Error fetching data for {ticker}: {e}")

        conn.close()
    except Exception as e:
        logger.error(f"Database error: {e}")
