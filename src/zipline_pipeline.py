import logging
import os

import pandas as pd
from dateutil.tz import UTC
from exchange_calendars import get_calendar

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


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
    csv_path = os.getenv("TRANSFORMED_CSV_PATH", "data/transformed_yahoo_data.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")
    logging.info(f"Loading CSV data from: {csv_path}")

    # Load CSV data
    data = pd.read_csv(csv_path)
    required_columns = ["date", "symbol", "open", "high", "low", "close", "volume"]
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in the CSV file: {missing_columns}")

    # Ensure dates are timezone-naive
    data["date"] = pd.to_datetime(data["date"]).dt.tz_localize(None)

    # Get trading calendar sessions
    trading_calendar = get_calendar("XNYS")  # NYSE calendar
    first_calendar_session = trading_calendar.first_session

    # Align start_session and end_session with calendar range
    start_session = max(start_session or first_calendar_session, first_calendar_session)
    end_session = min(
        end_session or trading_calendar.last_session, trading_calendar.last_session
    )
    logging.info(f"Using start session: {start_session}, end session: {end_session}")

    valid_sessions = trading_calendar.sessions_in_range(start_session, end_session)

    # Filter data to match valid sessions
    data = data[data["date"].isin(valid_sessions)]
    if data.empty:
        raise ValueError(
            f"No valid trading data matches the calendar sessions.\n"
            f"Start session: {start_session}\n"
            f"End session: {end_session}\n"
            f"CSV dates: {data['date'].unique()}"
        )

    # Group by symbol for multi-asset support
    grouped_data = []
    metadata = []
    sid = 1

    for symbol, group in data.groupby("symbol"):
        logging.info(f"Processing data for symbol: {symbol}")
        group = group.sort_values("date").set_index("date")

        # Add to grouped data
        grouped_data.append((sid, group))

        # Add metadata
        metadata.append(
            {
                "sid": sid,
                "symbol": symbol,
                "start_date": group.index.min(),
                "end_date": group.index.max(),
                "exchange": "XNYS",
            }
        )
        sid += 1

    # Write daily bar data
    logging.info("Writing daily bar data...")
    daily_bar_writer.write(grouped_data, show_progress=show_progress)
    logging.info("Daily bar data written successfully.")

    # Write metadata
    asset_metadata = pd.DataFrame(metadata)
    logging.info(f"Writing asset metadata:\n{asset_metadata}")
    asset_db_writer.write(asset_metadata)

    # Write splits and dividends
    logging.info("Writing splits and dividends...")
    splits = pd.DataFrame(columns=["sid", "effective_date", "ratio"])
    dividends = pd.DataFrame(
        columns=["sid", "ex_date", "pay_date", "record_date", "declared_date", "amount"]
    )
    adjustment_writer.write(splits=splits, dividends=dividends)
    logging.info("Splits and dividends written successfully.")
