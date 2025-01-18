import time
from functools import wraps

import yfinance as yf


def retry_on_failure(retries=3, delay=5):
    """
    Decorator to retry a function in case of failure.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    print(
                        f"Error: {e}. Retrying {attempts}/{retries} in {delay} seconds..."
                    )
                    time.sleep(delay)
            raise Exception(f"Failed after {retries} attempts.")

        return wrapper

    return decorator


@retry_on_failure(retries=3, delay=5)
def fetch_yfinance_data(ticker, start_date, end_date):
    """
    Fetch historical data from Yahoo Finance for the given ticker.
    """
    print(f"Fetching data for {ticker} from Yahoo Finance...")
    data = yf.download(ticker, start=start_date, end=end_date)
    if not data.empty:
        data = data[["Open", "Close", "Volume"]]
        data.columns = ["open", "close", "volume"]
        data.index = data.index.strftime("%Y-%m-%d")
        return data.to_dict(orient="index")
    else:
        print(f"No data retrieved for {ticker} from Yahoo Finance.")
        return {}
