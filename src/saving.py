import logging
import os

import pandas as pd


class DataSaver:
    @staticmethod
    def save_data(data, output_dir, name="merged_data"):
        """Save data to CSV and Parquet formats, cleaning up unnecessary columns."""
        csv_path = os.path.join(output_dir, f"{name}.csv")
        parquet_path = os.path.join(output_dir, f"{name}.parquet")

        try:
            # Drop unnecessary columns (e.g., 'Unnamed: 0')
            if "Unnamed: 0" in data.columns:
                data = data.drop(columns=["Unnamed: 0"])

            # Save to CSV
            data.to_csv(csv_path, index=True)
            logging.info(f"Saved data to {csv_path}")

            # Save to Parquet
            data.to_parquet(parquet_path, index=True)
            logging.info(f"Saved data to {parquet_path}")
        except Exception as e:
            logging.error(f"Failed to save data: {e}", exc_info=True)
            raise

    @staticmethod
    def validate_and_save(data, output_dir, name="validated_data"):
        """Validate for duplicate columns and save the data."""
        if data.columns.duplicated().any():
            duplicates = data.columns[data.columns.duplicated()].tolist()
            logging.error(f"Duplicate column names found: {duplicates}")
            raise ValueError(f"Duplicate column names detected: {duplicates}")

        DataSaver.save_data(data, output_dir, name)
