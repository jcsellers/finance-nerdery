import logging
import sqlite3
import time
from datetime import datetime

import pandas as pd
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_fixed

from src.utils import fill_missing_market_days


def fetch_yfinance_data(tickers, start_date, end_date="current", db_path=None):
    """
    Fetches historical data for a list of tickers using yfinance and stores it in SQLite.

    :param tickers: List of ticker symbols.
    :param start_date: Start date for historical data.
    :param end_date: End date for historical data. Defaults to "current".
    :param db_path: Path to the SQLite database.
    """
    logger = logging.getLogger("yfinance_fetcher")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    if end_date == "current":
        end_date = datetime.now().strftime("%Y-%m-%d")

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for ticker in tickers:
            logger.info(f"Fetching data for {ticker}")
            try:
                data = yf.download(ticker, start=start_date, end=end_date)
                if data.empty:
                    logger.warning(f"No data found for {ticker}")
                    continue

                # Normalize data
                data.reset_index(inplace=True)
                data = fill_missing_market_days(data, start_date, end_date)
                data["ticker"] = ticker
                data["source"] = "yfinance"
                data["is_filled"] = 0

                # Deduplicate and insert into SQLite
                for _, row in data.iterrows():
                    # Debugging log to check row contents
                    logger.debug(f"Processing row: {row}")

                    # Force scalar conversion
                    timestamp = row["Date"]
                    if isinstance(timestamp, pd.Timestamp):
                        timestamp = timestamp.to_pydatetime()
                    elif isinstance(timestamp, str):
                        timestamp = pd.to_datetime(timestamp)
                    else:
                        logger.error(f"Invalid timestamp format: {timestamp}")
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
                logger.info(f"Data stored for {ticker}")

            except Exception as e:
                logger.error(f"Error fetching data for {ticker}: {e}")

        conn.close()
    except Exception as e:
        logger.error(f"Database error: {e}")
