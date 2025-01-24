import logging
import os

# Ensure the logs directory exists
log_dir = os.path.join(os.path.dirname(__file__), "../logs")
os.makedirs(log_dir, exist_ok=True)

# Configure logging
log_file_path = os.path.join(log_dir, "test.log")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file_path, mode="w"),  # Log to the logs directory
    ],
)
logger = logging.getLogger(__name__)

# Test logging
logger.info("Test log entry to verify logging configuration.")
