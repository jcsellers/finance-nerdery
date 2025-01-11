import os
import sys

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Update PYTHONPATH
python_path = os.getenv("PYTHONPATH", "")
if python_path:
    sys.path = python_path.split(";") + sys.path  # Prepend custom paths
    os.environ["PYTHONPATH"] = ";".join(sys.path)

print("PYTHONPATH:", sys.path)
