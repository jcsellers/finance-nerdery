import json
import logging
import os
import sqlite3

import pandas as pd
import vectorbt as vbt

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Output to console
        logging.FileHandler(
            "logs/backtest.log", mode="w"
        ),  # Output to logs/backtest.log
    ],
)
logger = logging.getLogger("backtest_orchestrator")


def load_data(db_path, tickers, start_date, end_date):
    """
    Load financial data from the SQLite database.
    """
    conn = sqlite3.connect(db_path)
    schema = pd.read_sql("PRAGMA table_info(yahoo_data);", conn)
    date_col = schema[schema["name"].str.contains("Date", case=False, na=False)][
        "name"
    ].values[0]
    symbol_col = schema[schema["name"].str.contains("symbol", case=False, na=False)][
        "name"
    ].values[0]

    query = f"""
        SELECT "{date_col}" AS date, "{symbol_col}" AS symbol,
               "('SPY', 'Open')" AS open, "('SPY', 'High')" AS high,
               "('SPY', 'Low')" AS low, "('SPY', 'Close')" AS close,
               "('SPY', 'Volume')" AS volume
        FROM yahoo_data
        WHERE "{symbol_col}" IN ({','.join(f"'{t}'" for t in tickers)})
          AND "{date_col}" BETWEEN '{start_date}' AND '{end_date}';
    """
    data = pd.read_sql(query, conn, parse_dates=["date"])
    conn.close()

    if data.empty:
        logger.error("Loaded data is empty. Check database or query.")
        raise ValueError("No data available for the specified tickers and date range.")

    logger.debug(f"Loaded data shape: {data.shape}")
    logger.debug(f"Loaded data head:\n{data.head()}")

    data.set_index("date", inplace=True)
    data = data.pivot(columns="symbol")
    logger.info("Data loaded and pivoted for Vectorbt.")
    logger.debug(f"Pivoted data shape: {data.shape}")
    logger.debug(f"Pivoted data head:\n{data.head()}")
    return data


def run_buy_and_hold_strategy(data, initial_cash, fees):
    close = data["close"]

    # Handle missing data
    close = close.dropna(how="all", axis=1)  # Drop tickers with no data
    logger.debug(f"Filtered close data shape: {close.shape}")
    logger.debug(f"Filtered close columns: {close.columns}")

    # Check if data is valid
    if close.empty:
        logger.error(
            "No valid data available for backtesting. Skipping strategy execution."
        )
        raise ValueError("No valid data available for backtesting.")

    # Define signals
    entries = pd.DataFrame(False, index=close.index, columns=close.columns)
    entries.iloc[0] = True
    exits = pd.DataFrame(False, index=close.index, columns=close.columns)

    # Align signals to close
    entries = entries.reindex(
        index=close.index, columns=close.columns, fill_value=False
    )
    exits = exits.reindex(index=close.index, columns=close.columns, fill_value=False)

    # Verify alignment
    assert (
        close.shape == entries.shape == exits.shape
    ), f"Shape mismatch: close={close.shape}, entries={entries.shape}, exits={exits.shape}"

    # Run the backtest
    portfolio = vbt.Portfolio.from_signals(
        close,
        entries,
        exits,
        init_cash=initial_cash,
        fees=fees,
    )
    logger.info("Buy-and-Hold strategy executed.")
    return portfolio


def save_results(portfolio, output_dir):
    """
    Save backtest results to CSV files.

    Args:
        portfolio (vbt.Portfolio): Vectorbt portfolio object.
        output_dir (str): Directory to save the results.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Save performance metrics
    performance = portfolio.performance()
    performance.to_csv(f"{output_dir}/performance_metrics.csv")
    logger.info("Performance metrics saved.")

    # Save portfolio value over time
    portfolio.value().to_csv(f"{output_dir}/portfolio_value.csv")
    logger.info("Portfolio value saved.")

    # Save trades
    trades = portfolio.trades.records
    trades.to_csv(f"{output_dir}/trades.csv")
    logger.info("Trades saved.")


def orchestrate(config_path):
    """
    Main orchestrator function to run the backtesting pipeline.
    """
    with open(config_path, "r") as config_file:
        config = json.load(config_file)

    # Load configuration
    db_path = config["storage"]["SQLite"]
    tickers = config["tickers"]["Yahoo Finance"]
    start_date = config["date_ranges"]["start_date"]
    end_date = config["date_ranges"]["end_date"]
    if end_date == "current":
        from datetime import datetime

        end_date = datetime.now().strftime("%Y-%m-%d")
    output_dir = config["storage"]["output_dir"]
    initial_cash = config["capital_base"]
    fees = config.get("settings", {}).get("fees", 0.001)

    # Load data
    data = load_data(db_path, tickers, start_date, end_date)
    logger.debug(f"Data loaded for strategy:\n{data.head()}")

    # Run Buy-and-Hold strategy
    portfolio = run_buy_and_hold_strategy(data, initial_cash, fees)

    # Save results
    save_results(portfolio, output_dir)
    logger.info("Orchestrator completed successfully.")


if __name__ == "__main__":
    orchestrate("config/config.json")
