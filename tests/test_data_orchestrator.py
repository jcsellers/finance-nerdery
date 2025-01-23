import os

from data_orchestrator import Orchestrator


def test_data_orchestrator(tmp_path):
    config = f"""
    [sources]
    [sources.Yahoo_Finance]
    tickers = ["SPY", "UPRO"]
    start_date = "2020-01-01"
    end_date = "2020-12-31"

    [sources.FRED]
    api_key = "test_api_key"
    series_ids = ["BAMLH0A0HYM2"]

    [date_ranges]
    start_date = "2020-01-01"
    end_date = "2020-12-31"

    [output]
    output_dir = "{str(tmp_path).replace('\\', '\\\\')}"  # Escape backslashes

    [settings]
    missing_data_handling = "interpolate"
    """

    # Save the config as TOML
    config_path = os.path.join(tmp_path, "config.toml")
    with open(config_path, "w") as f:
        f.write(config)

    orchestrator = Orchestrator(config_path=config_path)
    orchestrator.run()
