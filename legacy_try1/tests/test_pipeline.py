import os

import pandas as pd
import pytest
from scripts.align_data import align_datasets


@pytest.fixture
def setup_test_environment(tmp_path):
    """Set up a temporary environment with mock cleaned data."""
    input_dir = tmp_path / "cleaned"
    output_dir = tmp_path / "aligned"
    input_dir.mkdir()
    output_dir.mkdir()

    # Create mock cleaned data
    files_data = {
        "test_data1.csv": {
            "Date": ["1990-01-02", "1990-01-03", "1990-01-05"],
            "Value": [100, 110, 120],
        },
        "test_data2.csv": {
            "Date": ["1990-01-02", "1990-01-04", "1990-01-06"],
            "Value": [200, 210, 220],
        },
    }

    for filename, data in files_data.items():
        df = pd.DataFrame(data)
        df.to_csv(input_dir / filename, index=False)

    return input_dir, output_dir, files_data


def test_align_datasets(setup_test_environment):
    """Test the alignment of datasets to a common date range."""
    input_dir, output_dir, files_data = setup_test_environment

    # Run alignment function
    align_datasets(input_dir=input_dir, output_dir=output_dir)

    # Verify output files are created
    aligned_files = [f"{os.path.splitext(f)[0]}_cleaned.csv" for f in files_data.keys()]
    for aligned_file in aligned_files:
        assert (
            output_dir / aligned_file
        ).exists(), f"Aligned file {aligned_file} not found."

    # Validate contents of aligned files
    for filename in aligned_files:
        aligned_df = pd.read_csv(output_dir / filename)

        # Check that the aligned file has a common date range
        common_dates = pd.date_range(start="1990-01-02", end="2025-01-03")
        assert len(aligned_df) == len(
            common_dates
        ), "Date range mismatch in aligned file."
        assert "Value" in aligned_df.columns, f"Value column missing in {filename}."


def test_align_handles_empty_directory(tmp_path):
    """Ensure alignment handles an empty input directory gracefully."""
    input_dir = tmp_path / "cleaned"
    output_dir = tmp_path / "aligned"
    input_dir.mkdir()
    output_dir.mkdir()

    # Run alignment function
    align_datasets(input_dir=input_dir, output_dir=output_dir)

    # Ensure no files are created in the output directory
    assert (
        len(os.listdir(output_dir)) == 0
    ), "Output directory should be empty for empty input directory."


def test_align_handles_invalid_data(tmp_path):
    """Ensure alignment handles invalid or corrupted data gracefully."""
    input_dir = tmp_path / "cleaned"
    output_dir = tmp_path / "aligned"
    input_dir.mkdir()
    output_dir.mkdir()

    # Create a corrupted file
    corrupted_file = input_dir / "corrupted.csv"
    with open(corrupted_file, "w") as f:
        f.write("This is not a valid CSV file")

    # Run alignment function
    align_datasets(input_dir=input_dir, output_dir=output_dir)

    # Ensure the corrupted file does not crash the function
    assert (
        len(os.listdir(output_dir)) == 0
    ), "Output directory should be empty for invalid input files."
