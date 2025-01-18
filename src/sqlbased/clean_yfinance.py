import os

import pandas as pd


def clean_yfinance_csv(file_path, output_path=None):
    """
    Clean the raw YFinance CSV file.

    Parameters:
        file_path (str): Path to the raw CSV file.
        output_path (str, optional): Path to save the cleaned CSV file. If None, does not save the cleaned file.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    # Load the data
    df = pd.read_csv(file_path)

    # Standardize column names
    df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]

    # Ensure Date column is in datetime format
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    else:
        raise ValueError("Date column is missing in the input data.")

    # Rename columns for database compatibility
    rename_map = {
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "adj_close": "adjusted_close",
        "volume": "volume",
    }
    df.rename(columns=rename_map, inplace=True)

    # Handle missing data
    df.dropna(
        subset=["date", "close"], inplace=True
    )  # Drop rows with critical missing data
    df.fillna(0, inplace=True)  # Fill non-critical missing data with 0

    # Optional: Sort by date
    df.sort_values("date", inplace=True)

    # Save the cleaned data
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"Cleaned data saved to {output_path}")

    return df


# Example usage
if __name__ == "__main__":
    raw_file_path = "SPY_raw.csv"
    cleaned_file_path = "SPY_cleaned.csv"
    cleaned_df = clean_yfinance_csv(raw_file_path, cleaned_file_path)
    print(cleaned_df.head())
