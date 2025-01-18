import logging
import os
import sqlite3
import time
from datetime import datetime

import pandas as pd
import yfinance as yf

from utils import fill_missing_market_days


def fetch_yfinance_data(
    tickers, start_date, end_date="current", db_path=None, intermediate_dir="./data"
):
    """
    Fetches historical data for a list of tickers using yfinance and stores it in SQLite.

    :param tickers: List of ticker symbols.
    :param start_date: Start date for historical data.
    :param end_date: End date for historical data. Defaults to "current".
    :param db_path: Path to the SQLite database.
    :param intermediate_dir: Directory to save intermediate CSV files.
    """
    # Set up logging
    logger = logging.getLogger("yfinance_fetcher")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    if end_date == "current":
        end_date = datetime.now().strftime("%Y-%m-%d")

    # Ensure the intermediate directory exists
    os.makedirs(intermediate_dir, exist_ok=True)

    if not db_path:
        logger.error("No database path provided. Exiting fetch operation.")
        return

    try:
        # Use context manager for database connection
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Ensure unique constraint on ticker and timestamp
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS historical_data (
                    ticker TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    adjusted_close REAL,
                    volume INTEGER,
                    source TEXT,
                    is_filled INTEGER,
                    PRIMARY KEY (ticker, timestamp)
                );
                """
            )

            for ticker in tickers:
                logger.info(f"Fetching data for {ticker}")
                try:
                    # Fetch data with retry logic
                    data = fetch_with_retry(ticker, start_date, end_date)

                    if data.empty:
                        logger.warning(f"No data found for {ticker}")
                        continue

                    # Flatten MultiIndex columns if present
                    if isinstance(data.columns, pd.MultiIndex):
                        data.columns = [
                            " ".join(col).strip() for col in data.columns.values
                        ]

                    # Normalize and handle missing market days
                    data.reset_index(inplace=True)

                    # Strip ticker suffix from column names (if present)
                    data.columns = [
                        col.split(" ")[0] if " " in col else col for col in data.columns
                    ]

                    logger.debug(
                        f"Fetched data structure after renaming columns:\n{data.head()}"
                    )
                    data = fill_missing_market_days(data, start_date, end_date)

                    if data.empty:
                        logger.warning(
                            f"Fetched data is empty after aligning with trading days for {ticker}"
                        )
                        continue

                    logger.debug(f"After filling missing days:\n{data.head()}")
                    data["ticker"] = ticker
                    data["source"] = "yfinance"
                    data["is_filled"] = 0

                    # Save intermediate data to CSV
                    intermediate_file = os.path.join(
                        intermediate_dir, f"{ticker}_data.csv"
                    )
                    data.to_csv(intermediate_file, index=False)
                    logger.info(
                        f"Saved intermediate data for {ticker} to {intermediate_file}"
                    )

                    # Validate and track rejected rows
                    rejected_rows = []
                    records_to_insert = []

                    for _, row in data.iterrows():
                        try:
                            timestamp = pd.to_datetime(row.get("Date"), errors="coerce")

                            # Convert timestamp to ISO 8601 string
                            if not pd.isna(timestamp):
                                timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")

                            # Check required fields
                            if pd.isna(timestamp):
                                rejection_reason = "Invalid or missing timestamp"
                            elif pd.isna(
                                row.get("Close")
                            ) or not pd.api.types.is_number(row.get("Close")):
                                rejection_reason = (
                                    f"Invalid data in row: Close={row.get('Close')}"
                                )
                            else:
                                rejection_reason = None

                            if rejection_reason:
                                logger.warning(
                                    f"Rejected row for ticker {ticker}: {row.to_dict()} | Reason: {rejection_reason}"
                                )
                                rejected_rows.append(
                                    {
                                        **row.to_dict(),
                                        "rejection_reason": rejection_reason,
                                    }
                                )
                                continue

                            # Check if the record already exists in the database
                            cursor.execute(
                                """
                                SELECT 1 FROM historical_data WHERE ticker = ? AND timestamp = ?
                                """,
                                (ticker, timestamp),
                            )
                            if cursor.fetchone():
                                logger.info(
                                    f"Skipping existing record for {ticker} at {timestamp}"
                                )
                                continue

                            records_to_insert.append(
                                (
                                    ticker,
                                    timestamp,
                                    row.get("Open", None),
                                    row.get("High", None),
                                    row.get("Low", None),
                                    row.get("Close", None),
                                    row.get("Adj Close", row.get("Close", None)),
                                    row.get("Volume", None),
                                    "yfinance",
                                    0,
                                )
                            )
                        except Exception as e:
                            logger.error(f"Error processing row: {row.to_dict()}\n{e}")

                    if rejected_rows:
                        rejected_file = os.path.join(
                            intermediate_dir, f"{ticker}_rejected_rows.csv"
                        )
                        pd.DataFrame(rejected_rows).to_csv(rejected_file, index=False)
                        logger.warning(
                            f"Rejected rows for {ticker} saved to {rejected_file}"
                        )

                    if records_to_insert:
                        # Batch insert into SQLite
                        cursor.executemany(
                            """
                            INSERT INTO historical_data (ticker, timestamp, open, high, low, close, adjusted_close, volume, source, is_filled)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT DO NOTHING;
                            """,
                            records_to_insert,
                        )

                        conn.commit()
                        logger.info(
                            f"Successfully stored data for {ticker}, rows inserted: {len(records_to_insert)}"
                        )
                    else:
                        logger.warning(f"No valid rows to insert for {ticker}")

                    time.sleep(1)  # Rate limiting to avoid API limits

                except Exception as e:
                    logger.error(f"Error fetching data for {ticker}: {e}")

    except Exception as e:
        logger.error(f"Database error: {e}")


def fetch_with_retry(ticker, start_date, end_date, max_retries=3):
    """Fetches data from yfinance with retry logic."""
    for attempt in range(max_retries):
        try:
            data = yf.download(ticker, start=start_date, end=end_date)
            if data.empty:
                raise ValueError("Empty data returned by yfinance")
            return data
        except Exception as e:
            logging.warning(f"Retry {attempt + 1} for {ticker} failed: {e}")
            time.sleep(2**attempt)
    raise RuntimeError(
        f"Failed to fetch data for {ticker} after {max_retries} attempts"
    )
