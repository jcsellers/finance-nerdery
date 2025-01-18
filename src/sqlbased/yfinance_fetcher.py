import logging
import os
import time

import pandas as pd
import yfinance as yf


def fetch_yfinance_data(
    tickers,
    start_date,
    end_date,
    intermediate_dir,
    max_retries=3,
    initial_delay=5,
    *args,
    **kwargs,
):
    """
    Fetch historical data from YFinance and save raw data for each ticker as a CSV file, with retry and backoff.

    Parameters:
        tickers (list): List of ticker symbols to fetch.
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        intermediate_dir (str): Directory to save CSV files.
        max_retries (int): Maximum number of retries for fetching data.
        initial_delay (int): Initial delay in seconds for backoff retries.
    """
    try:
        # Initialize logger
        logger = logging.getLogger("yfinance_fetcher")
        logger.info(f"Fetching YFinance data for tickers: {tickers}")

        # Ensure the directory exists
        if not os.path.isdir(intermediate_dir):
            raise ValueError(
                f"Provided intermediate_dir is not a directory: {intermediate_dir}"
            )

        if not os.path.exists(intermediate_dir):
            os.makedirs(intermediate_dir)
            logger.info(f"Created directory: {intermediate_dir}")

        # Retry logic for fetching data
        def download_with_retries(ticker):
            for attempt in range(1, max_retries + 1):
                try:
                    logger.info(
                        f"Attempting to fetch data for ticker {ticker} (Attempt {attempt}/{max_retries})"
                    )
                    data = yf.download(
                        tickers=ticker,
                        start=start_date,
                        end=end_date,
                        group_by="ticker",
                        auto_adjust=False,
                        progress=False,
                    )

                    if not data.empty:
                        logger.info(
                            f"Successfully fetched data for ticker {ticker} on attempt {attempt}"
                        )
                        return data

                except Exception as e:
                    logger.warning(f"Attempt {attempt} failed for ticker {ticker}: {e}")

                # Wait before retrying
                time.sleep(initial_delay * attempt)

            logger.error(
                f"Failed to fetch data for ticker {ticker} after {max_retries} attempts"
            )
            return None

        # Save raw data for each ticker
        for ticker in tickers:
            try:
                logger.info(f"Processing data for ticker: {ticker}")

                # Fetch data with retries
                df = download_with_retries(ticker)

                if df is None or df.empty:
                    logger.warning(
                        f"No valid data retrieved for ticker: {ticker}. Skipping."
                    )
                    continue

                # Reset index and save raw CSV
                df.reset_index(inplace=True)
                csv_path = os.path.join(intermediate_dir, f"{ticker}_raw.csv")
                df.to_csv(csv_path, index=False)
                logger.info(f"Saved raw CSV for ticker {ticker} to {csv_path}")

            except Exception as ticker_error:
                logger.error(f"Error processing ticker {ticker}: {ticker_error}")

        logger.info(f"Completed processing for {len(tickers)} tickers.")

    except Exception as e:
        logger.error(f"Error fetching data from YFinance: {e}")
        raise


if __name__ == "__main__":
    import argparse
    from datetime import datetime

    # Argument parsing
    parser = argparse.ArgumentParser(
        description="Fetch YFinance data and save as CSVs."
    )
    parser.add_argument(
        "--tickers", nargs="+", help="List of tickers to fetch", required=True
    )
    parser.add_argument(
        "--start_date", type=str, help="Start date in YYYY-MM-DD format", required=True
    )
    parser.add_argument(
        "--end_date", type=str, help="End date in YYYY-MM-DD format", required=True
    )
    parser.add_argument(
        "--intermediate_dir",
        type=str,
        help="Directory to save CSV files",
        default="../data",
    )
    parser.add_argument(
        "--max_retries", type=int, help="Maximum number of retries", default=3
    )
    parser.add_argument(
        "--initial_delay",
        type=int,
        help="Initial delay for backoff in seconds",
        default=5,
    )

    args = parser.parse_args()

    # Call the function with parsed arguments
    fetch_yfinance_data(
        tickers=args.tickers,
        start_date=args.start_date,
        end_date=args.end_date,
        intermediate_dir=args.intermediate_dir,
        max_retries=args.max_retries,
        initial_delay=args.initial_delay,
    )
