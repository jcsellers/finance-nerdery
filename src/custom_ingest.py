import os

import pandas as pd
from dateutil.tz import UTC
from exchange_calendars import get_calendar


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

    # Load CSV data
    data = pd.read_csv(csv_path)
    if not all(
        col in data.columns
        for col in ["date", "open", "high", "low", "close", "volume"]
    ):
        raise ValueError("Missing required columns in the CSV file.")

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

    valid_sessions = trading_calendar.sessions_in_range(start_session, end_session)

    # Filter data to match valid sessions
    data = data[data["date"].isin(valid_sessions)]
    if data.empty:
        raise ValueError(
            f"No valid trading data matches the calendar sessions.\n"
            f"Start session: {start_session}\n"
            f"End session: {end_session}\n"
            f"CSV dates: {data['date'].tolist()}"
        )

    # Prepare data for ingestion
    data = data.sort_values("date").set_index("date")

    # Write daily bar data
    daily_bar_writer.write([(1, data)], show_progress=show_progress)

    # Metadata for a single asset
    asset_metadata = pd.DataFrame(
        [
            {
                "sid": 1,
                "symbol": "SPY",
                "start_date": data.index.min(),
                "end_date": data.index.max(),
                "exchange": "XNYS",
            }
        ]
    )
    asset_db_writer.write(asset_metadata)

    # Write splits and dividends with required columns
    splits = pd.DataFrame(columns=["sid", "effective_date", "ratio"])
    dividends = pd.DataFrame(
        columns=["sid", "ex_date", "pay_date", "record_date", "declared_date", "amount"]
    )
    adjustment_writer.write(splits=splits, dividends=dividends)
