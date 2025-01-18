import logging
import os
import sqlite3
from datetime import datetime

import pandas as pd
import pandas_market_calendars as mcal
from fredapi import Fred
from utils import fill_missing_market_days


def fetch_fred_data(tickers, start_date, end_date, db_path, aliases):
    """
    Fetch data from FRED, align with NYSE trading days, and store in SQLite.

    :param tickers: List of FRED tickers to fetch.
    :param start_date: Start date for data fetching.
    :param end_date: End date for data fetching.
    :param db_path: Path to the SQLite database.
    :param aliases: Dictionary mapping tickers to aliases.
    """
    try:
        # Initialize FRED API
        fred_api_key = os.getenv("FRED_API_KEY")
        if not fred_api_key:
            raise ValueError("FRED_API_KEY is not set in the environment.")

        fred = Fred(api_key=fred_api_key)

        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for ticker in tickers:
            try:
                # Fetch data from FRED
                logging.info(f"Fetching data for {ticker} from FRED")
                raw_data = fred.get_series(
                    ticker, observation_start=start_date, observation_end=end_date
                )

                # Convert to DataFrame
                data = pd.DataFrame(raw_data, columns=["value"])
                data.reset_index(inplace=True)
                data.rename(columns={"index": "Date"}, inplace=True)

                # Align with NYSE trading days
                data = fill_missing_market_days(data, start_date, end_date)

                # Add ticker and alias columns
                data["symbol"] = ticker
                data["alias"] = aliases.get(ticker, "Unknown")
                data["source"] = "FRED"
                data["is_filled"] = data["value"].isna()

                # Validate value range (example: non-negative values)
                if (data["value"] < 0).any():
                    logging.warning(
                        f"Negative values found in data for {ticker}. Adjusting invalid values to NaN."
                    )
                    data.loc[data["value"] < 0, "value"] = None

                # Handle duplicates: Remove rows already in the database
                existing_dates_query = (
                    f"SELECT Date FROM economic_data WHERE symbol = '{ticker}'"
                )
                existing_dates = pd.read_sql_query(existing_dates_query, conn)[
                    "Date"
                ].tolist()
                data = data[~data["Date"].isin(existing_dates)]

                # Insert into SQLite database
                if not data.empty:
                    data.to_sql(
                        "economic_data",
                        conn,
                        if_exists="append",
                        index=False,
                        dtype={
                            "Date": "DATETIME",
                            "value": "REAL",
                            "symbol": "TEXT",
                            "alias": "TEXT",
                            "source": "TEXT",
                            "is_filled": "BOOLEAN",
                        },
                    )

                logging.info(
                    f"Successfully inserted data for {ticker} into the database."
                )

            except Exception as e:
                logging.error(f"Failed to fetch or process data for {ticker}: {e}")

        conn.close()

    except Exception as e:
        logging.error(f"Error in fetch_fred_data: {e}")


# Example usage in the pipeline
def main():
    logging.basicConfig(level=logging.INFO)

    # Configuration
    config_path = "config.json"
    with open(config_path, "r") as f:
        config = json.load(f)

    tickers = config["tickers"].get("FRED", [])
    aliases = config["aliases"].get("FRED", {})
    start_date = config["date_ranges"].get("start_date", "2020-01-01")
    end_date = config["date_ranges"].get("end_date", "current")
    db_path = config["storage"].get("SQLite", "data/finance_data.db")

    fetch_fred_data(tickers, start_date, end_date, db_path, aliases)


if __name__ == "__main__":
    main()
