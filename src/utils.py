import pandas as pd


def fill_missing_market_days(data, start_date, end_date):
    """
    Ensures the DataFrame includes all market days within the range.
    Missing days are filled with NaNs or forward-filled values.

    :param data: DataFrame with a 'Date' column as the index or a column.
    :param start_date: Start date of the desired range.
    :param end_date: End date of the desired range.
    :return: A DataFrame with all market days filled.
    """
    # Generate a range of all market days
    all_dates = pd.date_range(
        start=start_date, end=end_date, freq="B"
    )  # 'B' is business day frequency

    # Ensure 'Date' is the index for proper alignment
    if "Date" in data.columns:
        data.set_index("Date", inplace=True)

    # Reindex the DataFrame to include all dates and fill missing values
    data = data.reindex(all_dates)
    data.index.name = "Date"  # Ensure index is named 'Date'

    # Forward-fill or back-fill missing data
    data = data.ffill().bfill()

    # Reset index for consistency
    return data.reset_index()
