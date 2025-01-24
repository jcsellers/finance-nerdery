import json


def load_config(base_config_path, strategy_config_path):
    """
    Load and merge the base configuration with the strategy-specific configuration.

    Args:
        base_config_path (str): Path to the base configuration file.
        strategy_config_path (str): Path to the strategy-specific configuration file.

    Returns:
        dict: Merged configuration dictionary.
    """
    with open(base_config_path, "r") as base_file:
        base_config = json.load(base_file)

    with open(strategy_config_path, "r") as strategy_file:
        strategy_config = json.load(strategy_file)

    # Ensure strategy parameters are correctly nested
    strategy_params = strategy_config.get("parameters", {})
    merged_config = {
        **base_config,
        "parameters": strategy_params,
        "strategy_name": strategy_config["strategy_name"],
    }
    return merged_config
