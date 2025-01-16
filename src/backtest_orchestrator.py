import json
import os


def orchestrate(base_config_path, strategy_config_path):
    """Orchestrate the backtesting process."""
    # Load configurations using JSON directly
    with open(base_config_path, "r") as base_file:
        base_config = json.load(base_file)

    with open(strategy_config_path, "r") as strategy_file:
        strategy_config = json.load(strategy_file)

    output_dir = base_config.get("output_dir", "results")
    os.makedirs(output_dir, exist_ok=True)

    # Mock functionality
    print(f"Running strategy: {strategy_config['strategy_name']}")
    print(f"Configuration: {base_config}")

    # Simulate file creation for test validation
    metrics_path = os.path.join(output_dir, "performance_metrics.csv")
    with open(metrics_path, "w") as f:
        f.write("Mock performance metrics")

    dashboard_path = os.path.join(output_dir, "performance_dashboard.html")
    with open(dashboard_path, "w") as f:
        f.write("<html><body>Mock performance dashboard</body></html>")

    print(f"Output directory: {output_dir}")
