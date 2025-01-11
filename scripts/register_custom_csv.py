import os
from zipline.data.bundles import register
from zipline.data.bundles.csvdir import csvdir_equities

def register_custom_csv():
    """
    Registers a custom CSV data bundle for Zipline.
    """
    csvdir_path = os.getenv("CSV_PATH", "data/output/generated_data.csv")
    print(f"CSV_PATH: {csvdir_path}")

    if not os.path.exists(csvdir_path):
        raise FileNotFoundError(f"CSV directory does not exist: {csvdir_path}")

    register(
        'custom_csv',
        csvdir_equities(['daily'], csvdir_path),
        calendar_name='NYSE',  # Adjust as needed
    )
    print("Custom CSV bundle registered successfully.")

if __name__ == "__main__":
    try:
        register_custom_csv()
    except Exception as e:
        print(f"Error during registration: {e}")
        raise
