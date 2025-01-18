import time
from functools import wraps

from fredapi import Fred


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
def fetch_fred_data(series_id, start_date, end_date, api_key):
    """
    Fetch data from FRED for the given series ID.
    """
    fred = Fred(api_key=api_key)
    print(f"Fetching data for {series_id} from FRED...")
    data = fred.get_series(
        series_id, observation_start=start_date, observation_end=end_date
    )
    if not data.empty:
        return data.to_dict()
    else:
        print(f"No data retrieved for {series_id} from FRED.")
        return {}
