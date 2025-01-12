from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def generate_cash(start_date, end_date, constant_value):
    """
    Generate synthetic cash data with a constant value.

    Args:
        start_date (str): Start date in "YYYY-MM-DD" format.
        end_date (str): End date in "YYYY-MM-DD" format.
        constant_value (float): The constant value for the cash data.

    Returns:
        pd.DataFrame: Synthetic cash data.
    """
    dates = pd.date_range(start=start_date, end=end_date)
    data = {
        "date": dates,
        "value": [constant_value] * len(dates),
    }
    return pd.DataFrame(data)


def generate_linear(start_date, end_date, start_value, slope):
    """
    Generate synthetic linear data.

    Args:
        start_date (str): Start date in "YYYY-MM-DD" format.
        end_date (str): End date in "YYYY-MM-DD" format.
        start_value (float): Starting value of the data.
        slope (float): Linear slope (increment per day).

    Returns:
        pd.DataFrame: Synthetic linear data.
    """
    dates = pd.date_range(start=start_date, end=end_date)
    values = [start_value + slope * i for i in range(len(dates))]
    data = {
        "date": dates,
        "value": values,
    }
    return pd.DataFrame(data)
