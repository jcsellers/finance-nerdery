import json
import logging
import os
import sqlite3

import pandas as pd
import vectorbt as vbt

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_config(file_path):
    """
    Load a JSON configuration file.
    """
    with open(file_path, "r") as file:
        return json.load(file)


def sanitize_column_name(name):
    """
    Sanitize column names to replace problematic characters.
    """
    return name.replace("^", "_caret_").replace(" ", "_")


def generate_column_mapping(tickers):
    """
    Generate a dynamic column mapping based on the provided tickers.

    Args:
        tickers (list): List of tickers to include in the mapping.

    Returns:
        dict: Mapping of unconventional column names to standard names.
    """
    column_mapping = {"('date', '')": "date"}
    for ticker in tickers:
        sanitized_ticker = sanitize_column_name(ticker.lower())
        for field in ["open", "high", "low", "close", "volume"]:
            raw_column = f"('{ticker.lower()}', '{field}')"
            column_mapping[raw_column] = f"{sanitized_ticker}_{field}"
    return column_mapping


def escape_ticker(ticker):
    """
    Escape special characters in the ticker for SQLite queries.
    """
    return f"[{ticker}]"


def load_data_from_config(config, tickers):
    """
    Load data for specified tickers from SQLite, applying column mapping.

    Args:
        config (dict): Configuration containing data source details.
        tickers (list): List of tickers to load.

    Returns:
        pd.DataFrame: Combined financial data for all tickers.
    """
    data_frames = []
    column_mapping = generate_column_mapping(tickers)

    # Load from SQLite
    if "SQLite" in config["storage"]:
        try:
            conn = sqlite3.connect(config["storage"]["SQLite"])
            logging.info(f"Querying data from SQLite: {config['storage']['SQLite']}")

            # Generate dynamic SELECT clause
            select_clause = ", ".join(
                [f'"{col}" AS {alias}' for col, alias in column_mapping.items()]
            )
            query = f"""
                SELECT {select_clause}, ticker AS symbol
                FROM yahoo_data
                WHERE ticker IN ({','.join(f"'{t}'" for t in tickers)});
            """
            sqlite_data = pd.read_sql(query, conn, parse_dates=["date"])
            conn.close()

            if sqlite_data.empty:
                raise ValueError("No data found for the specified tickers in SQLite.")

            logging.info(f"Loaded {len(sqlite_data)} rows from SQLite.")
            logging.info(f"Data sample after query:\n{sqlite_data.head()}")
            data_frames.append(sqlite_data)
        except Exception as e:
            logging.warning(f"Failed to load data from SQLite: {e}")

    if not data_frames:
        raise ValueError("No data could be loaded from SQLite.")

    combined_data = pd.concat(data_frames)
    combined_data.set_index("date", inplace=True)

    # Filter by date range
    start_date = pd.Timestamp(config["date_ranges"]["start_date"])
    end_date = (
        pd.Timestamp(config["date_ranges"]["end_date"])
        if config["date_ranges"]["end_date"] != "current"
        else pd.Timestamp.now()
    )
    combined_data = combined_data.loc[start_date:end_date]
    logging.info(f"Data filtered for date range: {start_date} to {end_date}")

    return combined_data


def run_strategy_vectorbt(data, config):
    """
    Run a Buy-and-Hold strategy for multiple tickers using vectorbt.

    Args:
        data (pd.DataFrame): Financial data.
        config (dict): Configuration for the strategy.

    Returns:
        dict: Portfolio objects for each ticker.
    """
    portfolios = {}
    for ticker in config["tickers"]["Yahoo Finance"]:
        symbol_data = data[data["symbol"] == ticker]
        portfolios[ticker] = vbt.Portfolio.from_holding(
            close=symbol_data["close"], init_cash=config["capital_base"]
        )
    return portfolios


def orchestrate(base_config_path, strategy_config_path, config_path):
    """
    Orchestrate the backtesting process with multiple data sources.
    """
    # Load configurations
    base_config = load_config(base_config_path)
    strategy_config = load_config(strategy_config_path)
    additional_config = load_config(config_path)

    # Load data from SQLite and/or CSV
    tickers = additional_config["tickers"]["Yahoo Finance"]
    data = load_data_from_config(additional_config, tickers)

    # Run strategy for each ticker
    portfolios = run_strategy_vectorbt(data, additional_config)

    # Save results for each ticker
    output_dir = additional_config["storage"]["output_dir"]
    os.makedirs(output_dir, exist_ok=True)
    for ticker, portfolio in portfolios.items():
        metrics_path = os.path.join(output_dir, f"{ticker}_performance_metrics.csv")
        portfolio.performance().to_csv(metrics_path)
        logging.info(f"Performance metrics saved to: {metrics_path}")

        dashboard_path = os.path.join(
            output_dir, f"{ticker}_performance_dashboard.html"
        )
        portfolio.plot().write_html(dashboard_path)
        logging.info(f"Performance dashboard saved to: {dashboard_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the backtest orchestrator.")
    parser.add_argument(
        "--base-config", required=True, help="Path to base_config.json."
    )
    parser.add_argument(
        "--strategy-config", required=True, help="Path to strategy config."
    )
    parser.add_argument("--config", required=True, help="Path to additional config.")
    args = parser.parse_args()

    try:
        orchestrate(args.base_config, args.strategy_config, args.config)
        logging.info("Orchestrator completed successfully.")
    except Exception as e:
        logging.error(f"Orchestrator failed with error: {e}")
