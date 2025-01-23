import logging


class DataSaver:
    @staticmethod
    def save_data(data, output_dir, name="merged_data"):
        """Save data to CSV and Parquet formats."""
        csv_path = f"{output_dir}/{name}.csv"
        parquet_path = f"{output_dir}/{name}.parquet"

        try:
            # Save to CSV
            data.to_csv(csv_path, index=True)
            logging.info(f"Saved data to {csv_path}")

            # Save to Parquet
            data.to_parquet(parquet_path)
            logging.info(f"Saved data to {parquet_path}")
        except Exception as e:
            logging.error(f"Failed to save data: {e}", exc_info=True)
            raise
