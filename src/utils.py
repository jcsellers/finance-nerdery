import pandas as pd


def fill_missing_market_days(data, start_date, end_date):
    """
    Ensures the DataFrame includes all NYSE trading days within the range.
    Missing days are filled with NaNs or forward-filled values.

    :param data: DataFrame with a 'Date' column as the index or a column.
    :param start_date: Start date of the desired range.
    :param end_date: End date of the desired range.
    :return: A DataFrame with all NYSE trading days filled.
    """
    import logging

    import pandas_market_calendars as mcal

    logger = logging.getLogger("fill_missing_market_days")

    # Get the NYSE calendar
    nyse = mcal.get_calendar("NYSE")

    # Get all valid NYSE trading days within the range
    trading_days = nyse.schedule(start_date=start_date, end_date=end_date)
    all_trading_days = pd.to_datetime(trading_days.index)

    # Log the structure before reindexing
    logger.debug(f"Original DataFrame:\n{data.head()}")

    # Ensure 'Date' is the index for proper alignment
    if "Date" in data.columns:
        data.set_index("Date", inplace=True)

    # Reindex the DataFrame to include all NYSE trading days
    data = data.reindex(all_trading_days)

    # Ensure the index is named 'Date'
    data.index.name = "Date"

    # Forward-fill or back-fill missing data
    data = data.ffill().bfill()

    # Log the structure after reindexing
    logger.debug(f"Reindexed DataFrame:\n{data.head()}")

    # Reset index to restore 'Date' as a column
    data.reset_index(inplace=True)

    # Ensure 'Date' is consistent as a scalar
    if not pd.api.types.is_datetime64_any_dtype(data["Date"]):
        logger.error(
            "The 'Date' column is not in datetime format. Attempting to convert."
        )
        data["Date"] = pd.to_datetime(data["Date"], errors="coerce")

    return data
