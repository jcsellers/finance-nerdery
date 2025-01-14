import os

import pandas as pd
from zipline.assets import AssetDBWriter


def custom_ingest(
    environ,
    asset_db_writer,
    minute_bar_writer,
    daily_bar_writer,
    adjustment_writer,
    calendar,
    start_session,
    end_session,
    cache,
    show_progress,
    output_dir,
):
    """
    Custom ingest function for the Zipline bundle.
    """
    # CSV Path
    csv_path = os.getenv("TRANSFORMED_CSV_PATH", "src/data/transformed_yahoo_data.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")

    # Load CSV
    data = pd.read_csv(csv_path)

    # Validate Columns
    required_columns = ["date", "open", "high", "low", "close", "volume"]
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Format Data
    data["date"] = pd.to_datetime(data["date"])
    data = data.sort_values("date")
    data.set_index("date", inplace=True)

    # Write Daily Bar Data
    daily_bar_writer.write(
        [data[["open", "high", "low", "close", "volume"]]], show_progress=show_progress
    )

    # Write Asset Metadata
    asset_metadata = pd.DataFrame(
        {
            "sid": [0],
            "symbol": ["SPY"],
            "start_date": [data.index.min()],
            "end_date": [data.index.max()],
            "exchange": ["NYSE"],
        }
    )
    asset_db_writer.write(asset_metadata)

    # Write Corporate Actions (if available)
    splits = pd.DataFrame(
        {"sid": [0], "effective_date": [pd.Timestamp("2025-06-01")], "ratio": [0.5]}
    )
    dividends = pd.DataFrame(
        {
            "sid": [0],
            "ex_date": [pd.Timestamp("2025-03-01")],
            "pay_date": [pd.Timestamp("2025-03-15")],
            "amount": [0.50],
        }
    )
    adjustment_writer.write(splits=splits, dividends=dividends)
