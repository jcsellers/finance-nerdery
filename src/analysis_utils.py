import json


def analyze_results(performance_data, config_path="config/config.json"):
    """
    Analyze backtest results using the selected library.

    Args:
        performance_data (pd.DataFrame): Performance metrics from the backtest.
        config_path (str): Path to the configuration file.
    """
    # Load configuration
    with open(config_path, "r") as f:
        config = json.load(f)

    analysis_library = config.get("analysis_library", "pyfolio").lower()
    returns = performance_data["portfolio_value"].pct_change().dropna()

    if analysis_library == "pyfolio":
        import pyfolio as pf

        pf.create_full_tear_sheet(returns=returns)
    elif analysis_library == "quantstats":
        import quantstats as qs

        qs.reports.html(returns, output="quantstats_report.html")
        print("QuantStats report saved to quantstats_report.html")
    else:
        raise ValueError(f"Unknown analysis library: {analysis_library}")
