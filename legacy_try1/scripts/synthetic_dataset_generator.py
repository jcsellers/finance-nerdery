import logging
import os

import pandas as pd

# Paths
ALIGNED_DIR = "../data/aligned"

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def generate_linear_trend():
    """
    Generate a synthetic dataset with a linear trend.
    """
    dates = pd.date_range(
        start="1990-01-02", end="2025-01-03", freq="B"
    )  # Business days only
    values = [round(i * 0.1, 2) for i in range(len(dates))]  # Enforce precision
    df = pd.DataFrame({"Date": dates, "Value": values})
    return df


def generate_cash_dataset():
    """
    Generate a synthetic dataset with a constant value (cash).
    """
    dates = pd.date_range(
        start="1990-01-02", end="2025-01-03", freq="B"
    )  # Business days only
    values = [100] * len(dates)
    df = pd.DataFrame({"Date": dates, "Value": values})
    return df


def validate_synthetic_data(df, ticker):
    """
    Validate synthetic dataset conforms to the expected schema.
    """
    required_columns = {"Date", "Open", "High", "Low", "Close", "Volume", "ticker"}
    if set(df.columns) != required_columns:
        raise ValueError(
            f"{ticker}: Missing or extra columns. Found: {set(df.columns)}"
        )


def main():
    """
    Generate synthetic datasets and save them to the aligned directory.
    """
    os.makedirs(ALIGNED_DIR, exist_ok=True)

    # Generate synthetic datasets
    df_linear = generate_linear_trend()
    df_cash = generate_cash_dataset()

    # Add ticker column
    df_linear["ticker"] = "SYN_LINEAR"
    df_cash["ticker"] = "SYN_CASH"

    # Map Value to the expected schema
    for df, ticker in [(df_linear, "SYN_LINEAR"), (df_cash, "SYN_CASH")]:
        df["Open"] = df["Value"]
        df["Close"] = df["Value"]
        df["High"] = df["Value"]
        df["Low"] = df["Value"]
        df["Volume"] = 0  # Default value for Volume
        df.drop(columns=["Value"], inplace=True)
        validate_synthetic_data(df, ticker)

    # Save synthetic datasets
    linear_path = os.path.join(ALIGNED_DIR, "SYN_LINEAR_cleaned.csv")
    cash_path = os.path.join(ALIGNED_DIR, "SYN_CASH_cleaned.csv")

    df_linear.to_csv(linear_path, index=False)
    logging.info(f"Saved synthetic dataset: {linear_path}")

    df_cash.to_csv(cash_path, index=False)
    logging.info(f"Saved synthetic dataset: {cash_path}")


if __name__ == "__main__":
    main()
