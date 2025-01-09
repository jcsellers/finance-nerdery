import yaml
from csv_to_sqlite_import import process_all_csv_files

def load_config(config_path="config.yaml"):
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

if __name__ == "__main__":
    config = load_config("config.yaml")
    db_path = config["database"]["path"]
    process_all_csv_files("../aligned", db_path)
