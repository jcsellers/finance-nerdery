import logging
import os
import sys

import pytest
from dotenv import load_dotenv

# Add the `src/` directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

# Load environment variables from .env
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.env"))
load_dotenv(dotenv_path)

logging.info(f"Current Python Path: {sys.path}")


# Validate required environment variables
required_vars = ["FRED_API_KEY"]
missing_vars = [var for var in required_vars if os.getenv(var) is None]
if missing_vars:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(missing_vars)}"
    )

# Logging for test setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logging.info("Test setup: Environment variables loaded, Python path updated.")

# Shared fixtures
@pytest.fixture
def temp_output_dir(tmp_path):
    """Provide a temporary output directory for testing."""
    return str(tmp_path)
