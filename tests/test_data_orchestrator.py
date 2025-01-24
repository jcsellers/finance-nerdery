import os

import toml

from data_orchestrator import Orchestrator


def test_data_orchestrator(tmp_path):
    # Define the configuration as a Python dictionary
    config = {
        "sources": {
            "Yahoo_Finance": {
                "tickers": ["SPY", "UPRO"],
                "start_date": "2020-01-01",
                "end_date": "2020-12-31",
            },
            "FRED": {
                "api_key": "test_api_key",
                "series_ids": ["BAMLH0A0HYM2"],
            },
        },
        "date_ranges": {
            "start_date": "2020-01-01",
            "end_date": "2020-12-31",
        },
        "output": {
            "output_dir": "data",  # Explicitly specify the static `/data` directory
        },
        "settings": {
            "missing_data_handling": "interpolate",
        },
    }

    # Save the configuration to a TOML file
    config_path = os.path.join("data", "config.toml")
    os.makedirs("data", exist_ok=True)  # Ensure the directory exists
    with open(config_path, "w") as f:
        toml.dump(config, f)

    # Run the orchestrator with the generated config
    orchestrator = Orchestrator(config_path=config_path)
    orchestrator.run()
