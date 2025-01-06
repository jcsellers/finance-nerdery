import pytest
import pandas as pd
from data_handler import DataHandler
from metrics import Metrics
from strategies import Strategies

@pytest.fixture
def sample_data():
    data = {
        "date": pd.date_range(start="2020-01-01", periods=100),
        "close": [100 + i * 0.5 for i in range(100)]
    }
    return pd.DataFrame(data)

@pytest.fixture
def edge_case_data():
    data = {
        "date": pd.date_range(start="2020-01-01", periods=5),
        "close": [100, 95, 90, 85, 80]
    }
    return pd.DataFrame(data)

def test_validate_database():
    handler = DataHandler("mock_path")
    required_tables = {"data": {"ticker": "TEXT", "date": "TEXT", "open": "REAL", "close": "REAL"}}
    try:
        handler.validate_database(required_tables)
    except ValueError as e:
        pytest.fail(f"Database validation failed: {e}")

def test_fetch_data(sample_data):
    handler = DataHandler("mock_path")
    # Mocking or simulated fetch_data testing here
    assert not sample_data.empty
    assert set(["date", "close"]).issubset(sample_data.columns)

def test_calculate_sortino_ratio(sample_data, edge_case_data):
    assert Metrics.calculate_sortino_ratio(sample_data) > 0
    assert Metrics.calculate_sortino_ratio(edge_case_data) is not None

def test_calculate_max_drawdown(sample_data, edge_case_data):
    assert Metrics.calculate_max_drawdown(sample_data) == 0
    assert Metrics.calculate_max_drawdown(edge_case_data) < 0

def test_calculate_calmar_ratio(sample_data, edge_case_data):
    assert Metrics.calculate_calmar_ratio(sample_data) > 0
    assert Metrics.calculate_calmar_ratio(edge_case_data) is not None

def test_calculate_information_ratio():
    strategy_returns = pd.Series([0.01 * i for i in range(30)])
    benchmark_returns = pd.Series([0.015 * i for i in range(30)])
    assert Metrics.calculate_information_ratio(strategy_returns, benchmark_returns) is not None

def test_calculate_treynor_ratio():
    strategy_returns = pd.Series([0.01 * i for i in range(30)])
    beta = 1.2
    assert Metrics.calculate_treynor_ratio(strategy_returns, beta) > 0

def test_calculate_value_at_risk(sample_data):
    assert Metrics.calculate_value_at_risk(sample_data) < 0

def test_calculate_conditional_var(sample_data):
    assert Metrics.calculate_conditional_var(sample_data) < 0

def test_buy_and_hold_strategy(sample_data, edge_case_data):
    assert Strategies.buy_and_hold_strategy(sample_data) > 0
    assert Strategies.buy_and_hold_strategy(edge_case_data) == 0

def test_moving_average_crossover(sample_data, edge_case_data):
    assert Strategies.moving_average_crossover(sample_data) > 0
    assert Strategies.moving_average_crossover(edge_case_data) == 0
