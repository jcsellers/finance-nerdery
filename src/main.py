from data_handler import DataHandler
from metrics import Metrics
from strategies import Strategies
from outputs import Outputs
import yaml
import argparse
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path):
    """
    Load and validate configuration from a YAML file.

    Parameters:
        config_path (str): Path to the YAML configuration file.

    Returns:
        dict: Parsed configuration dictionary.

    Raises:
        ValueError: If required keys are missing or invalid.
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)

        # Required keys for validation
        required_keys = {
            "database": ["path"],
            "output": ["path"],
            "tickers": [],
            "date_range": ["start", "end"]
        }

        for section, keys in required_keys.items():
            if section not in config:
                raise ValueError(f"Missing required section: '{section}' in config.yaml")
            for key in keys:
                if key not in config[section]:
                    raise ValueError(f"Missing required key: '{section}.{key}' in config.yaml")

        logging.info("Configuration validated successfully.")
        return config
    except Exception as e:
        logging.error(f"Error loading or validating configuration: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run trading analysis pipeline.")
    parser.add_argument('--config', type=str, required=True, help="Path to the configuration file (YAML).")
    args = parser.parse_args()

    try:
        # Load and validate configuration
        config = load_config(args.config)

        db_path = config['database']['path']
        output_path = config['output']['path']
        tickers = config['tickers']
        date_range = config['date_range']

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Initialize DataHandler and validate database
        data_handler = DataHandler(db_path)
        required_tables = {
            "data": {"ticker": "TEXT", "date": "TEXT", "open": "REAL", "close": "REAL"}
        }
        data_handler.validate_database(required_tables)

        # Fetch data
        data = data_handler.fetch_data(tickers, date_range['start'], date_range['end'])

        # Metric calculations
        cagr = Metrics.calculate_cagr(data)
        volatility = Metrics.calculate_volatility(data)
        sharpe = Metrics.calculate_sharpe(data)

        # Strategy calculations
        strategy_returns = Strategies.buy_and_hold_strategy(data)

        # Alpha and Beta against benchmark (SPY)
        benchmark_returns = data[data['ticker'] == 'SPY']['close'].pct_change().dropna()
        alpha, beta = Metrics.calculate_alpha_beta(strategy_returns, benchmark_returns)

        # Generate output
        results = {
            "CAGR": cagr,
            "Volatility": volatility,
            "Sharpe Ratio": sharpe,
            "Alpha": alpha,
            "Beta": beta
        }
        Outputs.generate_json(results, output_path)

        logging.info(f"Pipeline completed successfully. Results saved to {output_path}.")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
