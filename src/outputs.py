import json
import logging
from jsonschema import validate, ValidationError
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Outputs:
    @staticmethod
    def generate_json(results, output_path):
        """
        Validate and save results to a JSON file.

        Parameters:
            results (dict): Dictionary containing results to save.
            output_path (str): Path to the output JSON file.

        Raises:
            ValueError: If results do not match the required schema.
        """
        schema = {
            "type": "object",
            "properties": {
                "CAGR": {"type": "number"},
                "Volatility": {"type": "number"},
                "Sharpe Ratio": {"type": "number"},
                "Alpha": {"type": ["number", "null"]},
                "Beta": {"type": ["number", "null"]},
                "Max Drawdown": {"type": ["number", "null"], "default": None},
                "Win Rate": {"type": ["number", "null"], "default": None}
            },
            "required": ["CAGR", "Volatility", "Sharpe Ratio"]
        }

        try:
            # Validate the results dictionary against the schema
            validate(instance=results, schema=schema)

            # Ensure the output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Write the results to the specified JSON file
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=4)
            logging.info(f"Results successfully written to {output_path}.")
        except ValidationError as e:
            logging.error(f"Validation error: {e.message}")
            raise ValueError("Results dictionary does not conform to the required schema.")
        except Exception as e:
            logging.error(f"Error generating output: {e}")
            raise
