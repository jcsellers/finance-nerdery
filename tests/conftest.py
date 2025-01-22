import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))


load_dotenv()  # Loads environment variables from .env file [1, 2, 4]
