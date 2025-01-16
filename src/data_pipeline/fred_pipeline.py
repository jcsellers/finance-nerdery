import logging
import sqlite3

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class FredPipeline:
    def __init__(self, api_key):
        self.api_key = api_key

    def fetch_data(self, series_id, start_date, end_date):
        """
        Fetch data from FRED for a specific series.

        Args:
            series_id (str): FRED series ID.
            start_date (str): Start date (YYYY-MM-DD).
            end_date (str): End date (YYYY-MM-DD).

        Returns:
            pd.DataFrame: Time-series data from FRED.
        """
        url = f"https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        observations = response.json().get("observations", [])
        if not observations:
            logger.warning(f"No data fetched for series {series_id}.")
            return pd.DataFrame()

        data = pd.DataFrame(observations)
        data["date"] = pd.to_datetime(data["date"])
        data["series_id"] = series_id
        return data[["date", "value", "series_id"]]

    def save_data(self, db_path, table_name, data):
        """
        Save data to an SQLite database.

        Args:
            db_path (str): Path to the SQLite database.
            table_name (str): Table name to save data.
            data (pd.DataFrame): Data to save.
        """
        try:
            conn = sqlite3.connect(db_path)
            data = data.drop_duplicates(subset=["date", "series_id"])
            data.to_sql(table_name, conn, if_exists="append", index=False)
            conn.close()
            logger.info(
                f"Data appended to SQLite database: {db_path}, table: {table_name}"
            )
        except Exception as e:
            logger.error(f"Error saving data to database: {e}")

    def run(self, series_ids, aliases, start_date, end_date, output_dir, db_path):
        """
        Fetch and save data for multiple series.

        Args:
            series_ids (list): List of FRED series IDs.
            aliases (dict): Mapping of series IDs to their aliases.
            start_date (str): Start date (YYYY-MM-DD).
            end_date (str): End date (YYYY-MM-DD).
            output_dir (str): Directory to save CSV files.
            db_path (str): Path to SQLite database.
        """
        all_data = []
        for series_id in series_ids:
            alias = aliases.get(series_id, series_id)
            try:
                logger.info(f"Fetching data for FRED series: {series_id} ({alias})")
                data = self.fetch_data(series_id, start_date, end_date)
                if not data.empty:
                    # Save to CSV
                    file_path = f"{output_dir}/fred_{alias}.csv"
                    data.to_csv(file_path, index=False)
                    logger.info(
                        f"FRED data for {series_id} ({alias}) saved to {file_path}."
                    )
                    all_data.append(data)
            except Exception as e:
                logger.error(f"Failed to fetch data for series {series_id}: {e}")

        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            self.save_data(db_path, "fred_data", combined_data)
