import logging

import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Strategies:
    @staticmethod
    def buy_and_hold_strategy(df):
        """
        Implements a buy-and-hold strategy with optional customization.
        Buys at the opening price of the first day and sells at the closing price of the last day.

        Parameters:
            df (DataFrame): A DataFrame with at least 'Date', 'Open', and 'Close' columns.

        Returns:
            float: The percentage return of the strategy.

        Raises:
            ValueError: If required columns are missing from the DataFrame.
        """
        try:
            required_columns = {"Date", "Open", "Close"}
            if not required_columns.issubset(df.columns):
                raise ValueError(f"DataFrame must contain columns: {required_columns}")

            df = df.sort_values(by="Date")
            buy_price = df["Open"].iloc[0]
            sell_price = df["Close"].iloc[-1]

            if buy_price <= 0 or sell_price <= 0:
                raise ValueError("Prices must be positive for valid returns.")

            return_percentage = ((sell_price - buy_price) / buy_price) * 100
            logging.info(f"Buy-and-hold strategy return: {return_percentage:.2f}%")
            return return_percentage
        except Exception as e:
            logging.error(f"Error in buy-and-hold strategy: {e}")
            raise

    @staticmethod
    def moving_average_crossover(df, short_window=50, long_window=200):
        """
        Implements a moving average crossover strategy.
        Buys when the short moving average crosses above the long moving average.
        Sells when the short moving average crosses below the long moving average.

        Parameters:
            df (DataFrame): A DataFrame with at least 'Date' and 'Close' columns.
            short_window (int): Period for the short moving average.
            long_window (int): Period for the long moving average.

        Returns:
            float: The percentage return of the strategy.

        Raises:
            ValueError: If required columns are missing from the DataFrame.
        """
        try:
            required_columns = {"Date", "Close"}
            if not required_columns.issubset(df.columns):
                raise ValueError(f"DataFrame must contain columns: {required_columns}")

            df = df.sort_values(by="Date").reset_index(drop=True)
            df["Short_MA"] = df["Close"].rolling(window=short_window).mean()
            df["Long_MA"] = df["Close"].rolling(window=long_window).mean()

            buy_signals = (df["Short_MA"] > df["Long_MA"]) & (
                df["Short_MA"].shift(1) <= df["Long_MA"].shift(1)
            )
            sell_signals = (df["Short_MA"] < df["Long_MA"]) & (
                df["Short_MA"].shift(1) >= df["Long_MA"].shift(1)
            )

            if not buy_signals.any() or not sell_signals.any():
                logging.warning("No valid crossover signals. Returning 0.")
                return 0

            buy_price = df.loc[buy_signals.idxmax(), "Close"]
            sell_price = df.loc[sell_signals.idxmax(), "Close"]

            return_percentage = ((sell_price - buy_price) / buy_price) * 100
            logging.info(
                f"Moving average crossover strategy return: {return_percentage:.2f}%"
            )
            return return_percentage
        except Exception as e:
            logging.error(f"Error in moving average crossover strategy: {e}")
            raise
