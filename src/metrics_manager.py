import logging
import os

import pandas as pd
import vectorbt as vbt  # Ensure we handle Portfolio objects correctly


class MetricsManager:
    def __init__(self, config):
        self.config = config
        self.output_dir = self.config["output"]["output_dir"]
        os.makedirs(self.output_dir, exist_ok=True)

    def save_results(self, results):
        """Save results from vectorbt portfolios."""
        logging.info("Saving results to output directory.")

        for result in results:
            strategy_name = result["strategy_name"]
            portfolio = result["portfolio"]
            entries = result.get("entries")  # Extract entries from the result
            exits = result.get("exits")  # Extract exits from the result
            strategy_dir = os.path.join(self.output_dir, strategy_name)
            os.makedirs(strategy_dir, exist_ok=True)

            # Save trade stats (includes key performance metrics like Sharpe, CAGR, etc.)
            try:
                trade_stats = portfolio.stats()
                logging.debug(f"Trade stats for {strategy_name}: {trade_stats}")
                if not isinstance(trade_stats, dict):
                    logging.error(
                        f"Trade stats for {strategy_name} is not a valid dictionary."
                    )
                    continue

                trade_stats_path = os.path.join(strategy_dir, "trade_stats.csv")
                pd.DataFrame([trade_stats]).to_csv(trade_stats_path, index=False)
                logging.info(
                    f"Saved trade stats for {strategy_name} to {trade_stats_path}."
                )
            except Exception as e:
                logging.error(
                    f"Failed to extract or save trade stats for {strategy_name}: {e}"
                )
                continue

            # Save signals (entries and exits)
            if entries is not None and exits is not None:
                signals_dir = os.path.join(strategy_dir, "signals")
                os.makedirs(signals_dir, exist_ok=True)

                entries_path = os.path.join(signals_dir, "entries.csv")
                exits_path = os.path.join(signals_dir, "exits.csv")

                try:
                    entries.to_frame(name="Entries").to_csv(entries_path)
                    exits.to_frame(name="Exits").to_csv(exits_path)
                    logging.info(f"Saved signals for {strategy_name} to {signals_dir}.")
                except Exception as e:
                    logging.error(f"Failed to save signals for {strategy_name}: {e}")

        logging.info("All results have been saved successfully.")
