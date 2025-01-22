import json
import logging
import os

from dotenv import dotenv_values, load_dotenv

from DataPipeline import DataPipeline

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self):
        """Load configuration from a JSON file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        with open(self.config_path, "r") as file:
            return json.load(file)

    def _validate_env_file(self):
        """Validate and log environment variables loaded from .env."""
        env_vars = dotenv_values()
        for key, value in env_vars.items():
            logger.info(f"Loaded env var: {key} = {value}")
        if not env_vars:
            logger.warning(
                "No environment variables found in .env or invalid .env format."
            )

    def run(self):
        """Run the orchestrator to execute the data pipeline."""
        logger.info("Starting the orchestrator...")
        try:
            load_dotenv()  # Load environment variables from .env
            self._validate_env_file()
            data_pipeline = DataPipeline(self.config)
            data_pipeline.run()
            logger.info("Data pipeline completed successfully.")
        except Exception as e:
            logger.error(f"Orchestrator encountered an error: {e}", exc_info=True)


if __name__ == "__main__":
    CONFIG_PATH = "./config/config.json"  # Adjust to your config location
    orchestrator = Orchestrator(CONFIG_PATH)
    orchestrator.run()
