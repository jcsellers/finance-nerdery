import yaml
from datetime import datetime
from data_handler import DataHandler
from database_validation import validate_database

def load_config(config_path="config.yaml"):
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def get_date_range(config):
    start_date = config["date_range"]["start"]
    end_date = config["date_range"]["end"]
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    return start_date, end_date

def main():
    # Load configuration
    config = load_config("config.yaml")
    db_path = config["database"]["path"]
    start_date, end_date = get_date_range(config)

    # Validate database
    validate_database(db_path)

    # Initialize DataHandler
    db_handler = DataHandler(db_path)

    # Example: Fetch data for a specific ticker
    tickers = config["tickers"]
    data = db_handler.fetch_data(tickers, start_date, end_date)

    # Additional logic here...

if __name__ == "__main__":
    main()

