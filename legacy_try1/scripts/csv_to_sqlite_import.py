import yaml
from database_utils import process_all_csv_files


def load_config(config_path="config.yaml"):
    """
    Load configuration from a YAML file.
    """
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


if __name__ == "__main__":
    config = load_config("config.yaml")
    db_path = config["database"]["path"]
    input_dir = "../data/aligned"
    process_all_csv_files(input_dir, db_path)
