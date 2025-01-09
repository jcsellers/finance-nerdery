import logging

import numpy as np
import statsmodels.api as sm

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Metrics:
    @staticmethod
    def calculate_sortino_ratio(df, risk_free_rate=0.02):
        """
        Calculate Sortino Ratio for a given dataset.

        Parameters:
            df (DataFrame): DataFrame with 'close' column.
            risk_free_rate (float): Annualized risk-free rate.

        Returns:
            float: Sortino Ratio.
        """
        try:
            returns = df["close"].pct_change().dropna()
            excess_returns = returns.mean() * 252 - risk_free_rate
            downside_deviation = np.sqrt(252) * np.std(returns[returns < 0])
            if downside_deviation == 0:
                logging.warning(
                    "Downside deviation is zero; Sortino Ratio is undefined."
                )
                return None
            return excess_returns / downside_deviation
        except Exception as e:
            logging.error(f"Error calculating Sortino Ratio: {e}")
            raise

    @staticmethod
    def calculate_max_drawdown(df):
        """
        Calculate Maximum Drawdown (MDD).

        Parameters:
            df (DataFrame): DataFrame with 'close' column.

        Returns:
            float: Maximum Drawdown as a percentage.
        """
        try:
            cumulative = (1 + df["close"].pct_change()).cumprod()
            rolling_max = cumulative.cummax()
            drawdown = (cumulative - rolling_max) / rolling_max
            return drawdown.min() * 100
        except Exception as e:
            logging.error(f"Error calculating Maximum Drawdown: {e}")
            raise

    @staticmethod
    def calculate_calmar_ratio(df):
        """
        Calculate Calmar Ratio using CAGR and MDD.

        Parameters:
            df (DataFrame): DataFrame with 'close' column.

        Returns:
            float: Calmar Ratio.
        """
        try:
            cagr = Metrics.calculate_cagr(df)
            mdd = Metrics.calculate_max_drawdown(df)
            if mdd == 0:
                logging.warning("Maximum Drawdown is zero; Calmar Ratio is undefined.")
                return None
            return cagr / abs(mdd)
        except Exception as e:
            logging.error(f"Error calculating Calmar Ratio: {e}")
            raise

    @staticmethod
    def calculate_information_ratio(strategy_returns, benchmark_returns):
        """
        Calculate Information Ratio.

        Parameters:
            strategy_returns (Series): Strategy returns.
            benchmark_returns (Series): Benchmark returns.

        Returns:
            float: Information Ratio.
        """
        try:
            active_returns = strategy_returns - benchmark_returns
            tracking_error = np.std(active_returns)
            if tracking_error == 0:
                logging.warning(
                    "Tracking error is zero; Information Ratio is undefined."
                )
                return None
            return np.mean(active_returns) / tracking_error
        except Exception as e:
            logging.error(f"Error calculating Information Ratio: {e}")
            raise

    @staticmethod
    def calculate_treynor_ratio(strategy_returns, beta, risk_free_rate=0.02):
        """
        Calculate Treynor Ratio.

        Parameters:
            strategy_returns (Series): Strategy returns.
            beta (float): Beta of the strategy.
            risk_free_rate (float): Annualized risk-free rate.

        Returns:
            float: Treynor Ratio.
        """
        try:
            excess_returns = strategy_returns.mean() * 252 - risk_free_rate
            if beta == 0:
                logging.warning("Beta is zero; Treynor Ratio is undefined.")
                return None
            return excess_returns / beta
        except Exception as e:
            logging.error(f"Error calculating Treynor Ratio: {e}")
            raise

    @staticmethod
    def calculate_value_at_risk(df, confidence_level=0.95):
        """
        Calculate Value at Risk (VaR).

        Parameters:
            df (DataFrame): DataFrame with 'close' column.
            confidence_level (float): Confidence level for VaR calculation.

        Returns:
            float: Value at Risk as a negative percentage.
        """
        try:
            returns = df["close"].pct_change().dropna()
            return np.percentile(returns, (1 - confidence_level) * 100) * 100
        except Exception as e:
            logging.error(f"Error calculating Value at Risk: {e}")
            raise

    @staticmethod
    def calculate_conditional_var(df, confidence_level=0.95):
        """
        Calculate Conditional Value at Risk (CVaR).

        Parameters:
            df (DataFrame): DataFrame with 'close' column.
            confidence_level (float): Confidence level for CVaR calculation.

        Returns:
            float: Conditional Value at Risk as a negative percentage.
        """
        try:
            returns = df["close"].pct_change().dropna()
            var = np.percentile(returns, (1 - confidence_level) * 100)
            return returns[returns <= var].mean() * 100
        except Exception as e:
            logging.error(f"Error calculating Conditional Value at Risk: {e}")
            raise

    @staticmethod
    def calculate_cagr(df):
        """
        Calculate Compound Annual Growth Rate (CAGR).

        Parameters:
            df (DataFrame): DataFrame with 'date' and 'close' columns.

        Returns:
            float: The CAGR value.
        """
        try:
            start_value = df["close"].iloc[0]
            end_value = df["close"].iloc[-1]
            num_years = (df["date"].iloc[-1] - df["date"].iloc[0]).days / 365.25
            return (end_value / start_value) ** (1 / num_years) - 1
        except Exception as e:
            logging.error(f"Error calculating CAGR: {e}")
            raise
