import logging
import sqlite3

import pandas as pd
import vectorbt as vbt

logger = logging.getLogger("backtest_orchestrator")


def run_strategy(config):

    print("Buy-and-Hold strategy script is running.")
    print("Buy-and-Hold strategy script is running.")
    print("Buy-and-Hold strategy script is running.")

    """
    Run the Buy-and-Hold strategy using Vectorbt.

    Args:
        config (dict): Configuration for the strategy.

    Returns:
        vbt.Portfolio: Vectorbt portfolio object.
    """
    # Load the data
    db_path = config["db_path"]
    target_asset = config["target_asset"]
    start_date = config["start_date"]
    end_date = config["end_date"]

    conn = sqlite3.connect(db_path)
    query = f"""
        SELECT "('Date', '')" AS date, "('SPY', 'Close')" AS close
        FROM yahoo_data
        WHERE "('symbol', '')" = '{target_asset}'
          AND "('Date', '')" BETWEEN '{start_date}' AND '{end_date}';
    """
    data = pd.read_sql(query, conn, parse_dates=["date"])
    conn.close()

    if data.empty:
        logger.error(
            f"No data available for {target_asset} in the specified date range."
        )
        raise ValueError(
            f"No data available for {target_asset} in the specified date range."
        )

    # Start from the first available trading day
    data.set_index("date", inplace=True)
    close = data["close"].sort_index()

    # Debugging: Log the shape and preview of close data
    logger.debug(f"Close data shape: {close.shape}")
    logger.debug(f"Close data head:\n{close.head()}")

    # Define buy-and-hold signals
    entries = pd.DataFrame(False, index=close.index, columns=[target_asset])
    entries.iloc[0] = True  # Buy on the first available date
    exits = pd.DataFrame(False, index=close.index, columns=[target_asset])  # Never sell

    # Align entries and exits to close
    entries = entries.reindex(
        index=close.index, columns=close.columns, fill_value=False
    )
    exits = exits.reindex(index=close.index, columns=close.columns, fill_value=False)

    # Debugging: Log the shape and preview of entries and exits
    logger.debug(f"Entries shape: {entries.shape}, Exits shape: {exits.shape}")
    logger.debug(f"Entries head:\n{entries.head()}")
    logger.debug(f"Exits head:\n{exits.head()}")

    # Verify alignment
    assert (
        close.shape == entries.shape == exits.shape
    ), f"Shape mismatch: close={close.shape}, entries={entries.shape}, exits={exits.shape}"

    # Run the backtest
    portfolio = vbt.Portfolio.from_signals(
        close,
        entries,
        exits,
        init_cash=config.get("capital_base", 100000),
        fees=config.get("fees", 0.001),
    )
    logger.info("Buy-and-Hold strategy executed.")
    return portfolio
