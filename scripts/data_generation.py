import numpy as np
import pandas as pd

def generate_linear_trend(start_price, start_date, days, ticker="SYN_LINEAR", noise=0.01):
    """
    Generates a synthetic dataset with a linear trend.
    """
    timestamps = pd.date_range(start=start_date, periods=days, freq="D")
    trend = np.linspace(start_price, start_price * 1.5, days)
    noise_array = np.random.normal(0, noise, size=days)

    prices = trend + noise_array
    highs = prices * (1 + np.random.uniform(0.01, 0.03, size=days))
    lows = prices * (1 - np.random.uniform(0.01, 0.03, size=days))
    volumes = np.random.randint(1000, 5000, size=days)

    return pd.DataFrame({
        "timestamp": timestamps,
        "ticker": ticker,
        "open": prices,
        "high": highs,
        "low": lows,
        "close": prices,
        "volume": volumes,
    })

def generate_cash_asset(initial_balance, start_date, days, ticker="SYN_CASH"):
    """
    Generates a synthetic cash dataset with consistent values.
    """
    timestamps = pd.date_range(start=start_date, periods=days, freq="D")
    return pd.DataFrame({
        "timestamp": timestamps,
        "ticker": ticker,
        "open": [initial_balance] * days,
        "high": [initial_balance] * days,
        "low": [initial_balance] * days,
        "close": [initial_balance] * days,
        "volume": [0] * days,
    })
