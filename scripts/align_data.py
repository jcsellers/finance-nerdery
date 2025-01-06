import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Directories
CLEANED_DIR = "../data/cleaned"
ALIGNED_DIR = "../data/aligned"

# Alignment range
START_DATE = "1990-01-02"
END_DATE = "2025-01-03"

def align_datasets():
    # Ensure aligned directory exists
    os.makedirs(ALIGNED_DIR, exist_ok=True)
    
    # Get all cleaned files
    files = [f for f in os.listdir(CLEANED_DIR) if f.endswith("_cleaned.csv")]
    common_dates = pd.date_range(start=START_DATE, end=END_DATE)

    for file in files:
        try:
            # Load data
            filepath = os.path.join(CLEANED_DIR, file)
            df = pd.read_csv(filepath)

            # Ensure 'Date' is datetime
            df['Date'] = pd.to_datetime(df['Date'])

            # Set 'Date' as index for alignment
            df.set_index('Date', inplace=True)

            # Reindex to the common date range
            df = df.reindex(common_dates)

            # Forward-fill and backward-fill missing data
            df.ffill(inplace=True)
            df.bfill(inplace=True)

            # Reset index and save the aligned dataset
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'Date'}, inplace=True)
            aligned_filepath = os.path.join(ALIGNED_DIR, file)
            df.to_csv(aligned_filepath, index=False)
            
            logging.info(f"Aligned and saved: {file}")
        
        except Exception as e:
            logging.error(f"Error aligning {file}: {e}")

if __name__ == "__main__":
    logging.info("Starting dataset alignment...")
    align_datasets()
    logging.info("Dataset alignment completed.")
