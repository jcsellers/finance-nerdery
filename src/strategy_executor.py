import importlib
import logging

import vectorbt as vbt


class StrategyExecutor:
    def __init__(self, config):
        self.config = config

    def load_strategy(self, module_name):
        """Dynamically load a strategy module."""
        try:
            strategy_module = importlib.import_module(module_name)
            if not hasattr(strategy_module, "generate_signals"):
                raise AttributeError(
                    f"Strategy {module_name} must define 'generate_signals'."
                )
            return strategy_module
        except ImportError as e:
            logging.error(f"Failed to load strategy module {module_name}: {e}")
            raise

    def execute_strategy(self, data, strategy_config):
        """Execute a single strategy dynamically."""
        module_name = strategy_config["module"]
        parameters = strategy_config.get("parameters", {})

        # Load the strategy module
        strategy_module = self.load_strategy(module_name)

        # Generate signals
        signals = strategy_module.generate_signals(data, **parameters)

        # Backtest using vectorbt
        portfolio = vbt.Portfolio.from_signals(
            close=data["Close"], entries=signals["entries"], exits=signals["exits"]
        )

        # Collect performance metrics
        results = portfolio.performance()

        # Include additional custom metrics if defined
        if hasattr(strategy_module, "custom_metrics"):
            additional_metrics = strategy_module.custom_metrics(portfolio)
            results.update(additional_metrics)

        return {
            "strategy": strategy_config["strategy_name"],
            "performance": results,
            "trade_stats": portfolio.stats(),
            "signals": signals,
        }

    def execute(self, data):
        """Execute all strategies and benchmarks defined in the configuration."""
        results = []

        # Execute strategies
        for strategy_config in self.config.get("strategies", []):
            try:
                results.append(self.execute_strategy(data, strategy_config))
            except Exception as e:
                logging.error(
                    f"Error executing strategy {strategy_config['strategy_name']}: {e}"
                )

        # Execute benchmarks
        for benchmark_config in self.config.get("benchmarks", []):
            try:
                results.append(self.execute_strategy(data, benchmark_config))
            except Exception as e:
                logging.error(
                    f"Error executing benchmark {benchmark_config['strategy_name']}: {e}"
                )

        return results
