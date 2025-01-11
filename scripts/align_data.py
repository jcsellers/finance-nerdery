import logging
import os

import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def align_datasets(input_dir="../data/cleaned", output_dir="../data/aligned"):
    """
    Align datasets in the input directory to a common date range and save to the output directory.

    Parameters:
        input_dir (str): Path to the directory containing cleaned data.
        output_dir (str): Path to the directory to save aligned data.
    """
    os.makedirs(output_dir, exist_ok=True)

    files = [f for f in os.listdir(input_dir) if f.endswith(".csv")]
    common_dates = pd.date_range(start="1990-01-02", end="2025-01-03")

    for file in files:
        try:
            filepath = os.path.join(input_dir, file)
            df = pd.read_csv(filepath)

            # Ensure 'Date' is datetime and set as index
            df["Date"] = pd.to_datetime(df["Date"])
            df.set_index("Date", inplace=True)

            # Handle ticker assignment
            ticker = (
                df["ticker"].iloc[0]
                if "ticker" in df.columns
                else os.path.splitext(file)[0]
            )

            # Reindex and fill missing values
            df = df.reindex(common_dates)
            df.ffill(inplace=True)
            df.bfill(inplace=True)

            # Reset index and save the aligned dataset
            df.reset_index(inplace=True)
            df.rename(columns={"index": "Date"}, inplace=True)

            # Add the 'ticker' column back
            df["ticker"] = ticker

            # Define aligned filepath and avoid duplication
            aligned_filepath = os.path.join(output_dir, f"{ticker}_aligned.csv")
            if os.path.exists(aligned_filepath):
                os.remove(aligned_filepath)

            df.to_csv(aligned_filepath, index=False)
            logging.info(f"Aligned and saved: {aligned_filepath}")

        except Exception as e:
            logging.error(f"Error aligning {file}: {e}")
