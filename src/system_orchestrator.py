import logging

import toml

from data_orchestrator import DataOrchestrator
from metrics_manager import MetricsManager
from strategy_executor import StrategyExecutor


class SystemOrchestrator:
    def __init__(self, config_path):
        """Initialize the orchestrator with configuration."""
        self.config = self.load_config(config_path)

        # Initialize components
        self.data_orchestrator = DataOrchestrator(self.config)
        self.strategy_executor = StrategyExecutor(self.config)
        self.metrics_manager = MetricsManager(self.config)

    @staticmethod
    def load_config(config_path):
        """Load and validate configuration from TOML file."""
        try:
            return toml.load(config_path)
        except Exception as e:
            logging.error(f"Failed to load configuration: {e}")
            raise

    def run(self):
        """Run the full pipeline from data acquisition to results saving."""
        logging.info("Starting System Orchestrator pipeline.")

        # Step 1: Fetch and align data
        try:
            logging.info("Executing data pipeline.")
            merged_data = self.data_orchestrator.run()
        except Exception as e:
            logging.error(f"Data pipeline failed: {e}")
            raise

        # Step 2: Execute strategies and benchmarks
        try:
            logging.info("Executing strategies and benchmarks.")
            results = self.strategy_executor.execute(merged_data)
        except Exception as e:
            logging.error(f"Strategy execution failed: {e}")
            raise

        # Step 3: Save metrics and logs
        try:
            logging.info("Saving metrics and logs.")
            self.metrics_manager.save_results(results)
        except Exception as e:
            logging.error(f"Metrics saving failed: {e}")
            raise

        logging.info("System Orchestrator pipeline completed successfully.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the system orchestrator.")
    parser.add_argument(
        "--config", required=True, help="Path to the configuration file."
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("orchestrator.log", mode="a"),
        ],
    )

    orchestrator = SystemOrchestrator(config_path=args.config)
    orchestrator.run()
