import subprocess

import pytest


@pytest.fixture
def real_config_path():
    """Path to the real configuration file."""
    return "config/strategies/buy_and_hold_real.json"


@pytest.fixture
def real_data_path():
    """Path to the real CSV file."""
    return "data/csv_files/yahoo_data.csv"


def test_orchestrator_run(real_config_path, real_data_path):
    """Test the orchestrator by running the actual script from the command line."""
    # Construct the command to run the script as it would be from the command line
    command = [
        "python",
        "src/strategy_orchestrator.py",
        "--config",
        real_config_path,
        "--data",
        real_data_path,
    ]

    # Run the command and capture the output
    result = subprocess.run(command, capture_output=True, text=True)

    # Check if the command ran successfully (exit code 0)
    assert result.returncode == 0, f"Command failed with exit code {result.returncode}"

    # Check that the output contains expected values
    assert "CAGR [%]" in result.stdout
    assert (
        float(result.stdout.split("CAGR [%]")[1].split()[0]) > 0
    )  # Ensure CAGR is positive

    # Optionally, you can print the output for debugging purposes
    print(result.stdout)
