import sqlite3
import pandas as pd
import logging
import yaml
from outputs import Outputs  # Ensure this module exists in your project

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_database(db_path, required_schema):
    """
    Validate the database schema against the required schema.

    Parameters:
        db_path (str): Path to the SQLite database file.
        required_schema (dict): Expected schema in the format {table_name: {column_name: column_type}}.

    Raises:
        ValueError: If schema validation fails.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        for table, columns in required_schema.items():
            cursor.execute(f"PRAGMA table_info({table});")
            schema = {row[1]: row[2].upper() for row in cursor.fetchall()}
            for column, dtype in columns.items():
                if column not in schema:
                    raise ValueError(f"Table '{table}' is missing column '{column}'. Please add it.")
                if dtype != schema[column]:
                    # Allow 'DATE' as a compatible type for 'TEXT'
                    if column == "date" and schema[column] == "DATE":
                        logging.warning(f"Column 'date' in table '{table}' has type 'DATE'. Treating as compatible.")
                        continue
                    raise ValueError(f"Column '{column}' in table '{table}' has incorrect type. "
                                     f"Expected '{dtype}', found '{schema[column]}'.")
        logging.info("Database schema validated successfully.")
    finally:
        conn.close()

def load_config(config_path):
    """
    Load and validate configuration from a YAML file.

    Parameters:
        config_path (str): Path to the YAML configuration file.

    Returns:
        dict: Parsed configuration dictionary.

    Raises:
        ValueError: If required keys are missing or invalid.
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)

        # Required keys for validation
        required_keys = {
            "database": ["path"],
            "output": ["path"],
            "tickers": [],
            "date_range": ["start", "end"],
            "benchmarks": []
        }

        for section, keys in required_keys.items():
            if section not in config:
                raise ValueError(f"Missing required section: '{section}' in config.yaml")
            for key in keys:
                if key not in config[section]:
                    raise ValueError(f"Missing required key: '{section}.{key}' in config.yaml")

        logging.info("Configuration validated successfully.")
        return config
    except Exception as e:
        logging.error(f"Error loading or validating configuration: {e}")
        raise

def calculate_metrics(df, risk_free_rate=0.02):
    """
    Calculate financial metrics for the given DataFrame.

    Parameters:
        df (DataFrame): DataFrame containing 'date' and 'close' columns.
        risk_free_rate (float): Risk-free rate for calculations.

    Returns:
        dict: Calculated metrics.
    """
    # Ensure 'date' is datetime and sort by date
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Calculate daily returns
    df['returns'] = df['close'].pct_change().dropna()

    # Calculate metrics
    cagr = calculate_cagr(df)
    volatility = calculate_volatility(df)
    sharpe_ratio = calculate_sharpe_ratio(df, risk_free_rate)
    max_drawdown = calculate_max_drawdown(df)
    sortino_ratio = calculate_sortino_ratio(df, risk_free_rate)

    return {
        "CAGR": cagr,
        "Volatility": volatility,
        "Sharpe Ratio": sharpe_ratio,
        "Max Drawdown": max_drawdown,
        "Sortino Ratio": sortino_ratio
    }

def calculate_cagr(df):
    """
    Calculate Compound Annual Growth Rate (CAGR).

    Parameters:
        df (DataFrame): DataFrame containing 'date' and 'close' columns.

    Returns:
        float: CAGR value.
    """
    n_years = (df['date'].iloc[-1] - df['date'].iloc[0]).days / 365.25
    cagr = (df['close'].iloc[-1] / df['close'].iloc[0]) ** (1 / n_years) - 0.02
    return cagr

def calculate_volatility(df):
    """
    Calculate annualized volatility.

    Parameters:
        df (DataFrame): DataFrame containing 'returns' column.

    Returns:
        float: Annualized volatility.
    """
    return df['returns'].std() * (252 ** 0.5)

def calculate_sharpe_ratio(df, risk_free_rate):
    """
    Calculate Sharpe Ratio.

    Parameters:
        df (DataFrame): DataFrame containing 'returns' column.
        risk_free_rate (float): Risk-free rate for calculations.

    Returns:
        float: Sharpe Ratio.
    """
    excess_return = df['returns'].mean() - risk_free_rate / 252
    return (excess_return / df['returns'].std()) * (252 ** 0.5)

def calculate_max_drawdown(df):
    """
    Calculate Maximum Drawdown (MDD).

    Parameters:
        df (DataFrame): DataFrame containing 'close' column.

    Returns:
        float: Maximum Drawdown as a percentage.
    """
    cumulative = (1 + df['returns']).cumprod()
    rolling_max = cumulative.cummax()
    drawdown = (cumulative - rolling_max) / rolling_max
    return drawdown.min() * 100

def calculate_sortino_ratio(df, risk_free_rate):
    """
    Calculate Sortino Ratio.

    Parameters:
        df (DataFrame): DataFrame containing 'returns' column.
        risk_free_rate (float): Risk-free rate for calculations.

    Returns:
        float: Sortino Ratio.
    """
    negative_returns = df['returns'][df['returns'] < 0]
    downside_deviation = negative_returns.std() * (252 ** 0.5)
    excess_return = df['returns'].mean() - risk_free_rate / 252
    return (excess_return / downside_deviation) * (252 ** 0.5)

def compare_with_benchmark(df, benchmark_df):
    """
    Compare strategy metrics with benchmark metrics.

    Parameters:
        df (DataFrame): DataFrame containing strategy 'returns'.
        benchmark_df (DataFrame): DataFrame containing benchmark 'returns'.

    Returns:
        dict: Comparison metrics.
    """
    strategy_metrics = calculate_metrics(df)
    benchmark_metrics = calculate_metrics(benchmark_df)

    comparison = {
        "Strategy": strategy_metrics,
        "Benchmark": benchmark_metrics,
        "Alpha": strategy_metrics["CAGR"] - benchmark_metrics["CAGR"],
        "Beta": df['returns'].cov(benchmark_df['returns']) / benchmark_df['returns'].var()
    }

    return comparison

def calculate_metrics_and_generate_output(db_path, output_path, tickers, date_range, benchmarks):
    """
    Process the validated database
::contentReference[oaicite:0]{index=0}
 
