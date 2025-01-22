import pandas as pd
import pytest

from src.strategy_orchestrator import BuyAndHoldStrategy, CashLinearAlternationStrategy


@pytest.fixture
def synthetic_linear_data():
    """Fixture for synthetic linear test data."""
    return pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=10, freq="D"),
            "synthetic_linear": [100 + i for i in range(10)],
        }
    ).set_index("date")


@pytest.fixture
def synthetic_cash_data():
    """Fixture for synthetic cash test data."""
    return pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=10, freq="D"),
            "synthetic_cash": [1000 - i * 10 for i in range(10)],
        }
    ).set_index("date")


@pytest.fixture
def empty_data():
    """Fixture for an empty dataset."""
    return pd.DataFrame(
        columns=["date", "synthetic_linear", "synthetic_cash"]
    ).set_index("date")


@pytest.fixture
def missing_timestamps_data():
    """Fixture for data with missing timestamps."""
    return pd.DataFrame(
        {
            "date": ["2023-01-01", "2023-01-03", "2023-01-04", "2023-01-06"],
            "synthetic_linear": [100, 102, 103, 105],
            "synthetic_cash": [1000, 990, 980, 970],
        }
    ).set_index("date")


def test_buy_and_hold_synthetic_linear(synthetic_linear_data):
    """Test BuyAndHoldStrategy with synthetic linear data."""
    # Configuration for Buy and Hold strategy
    config = {"target_asset": "synthetic_linear", "order_size": 100}

    strategy = BuyAndHoldStrategy()
    strategy.initialize(config)

    # Run the strategy
    strategy.run(synthetic_linear_data)

    # Expected outcome
    expected_results = [{"asset": "synthetic_linear", "price": 100, "units": 100}]
    assert strategy.output() == expected_results


def test_buy_and_hold_synthetic_cash(synthetic_cash_data):
    """Test BuyAndHoldStrategy with synthetic cash data."""
    # Configuration for Buy and Hold strategy
    config = {"target_asset": "synthetic_cash", "order_size": 50}

    strategy = BuyAndHoldStrategy()
    strategy.initialize(config)

    # Run the strategy
    strategy.run(synthetic_cash_data)

    # Expected outcome
    expected_results = [{"asset": "synthetic_cash", "price": 1000, "units": 50}]
    assert strategy.output() == expected_results


def test_empty_dataset(empty_data):
    """Test strategy with an empty dataset."""
    config = {"target_asset": "synthetic_linear", "order_size": 100}

    strategy = BuyAndHoldStrategy()
    strategy.initialize(config)

    with pytest.raises(IndexError):
        strategy.run(empty_data)


def test_missing_timestamps_dataset(missing_timestamps_data):
    """Test strategy with missing timestamps in the dataset."""
    config = {"target_asset": "synthetic_linear", "order_size": 100}

    strategy = BuyAndHoldStrategy()
    strategy.initialize(config)

    # Run the strategy
    strategy.run(missing_timestamps_data)

    # Expected outcome
    expected_results = [{"asset": "synthetic_linear", "price": 100, "units": 100}]
    assert strategy.output() == expected_results


def test_cash_linear_alternation(synthetic_linear_data, synthetic_cash_data):
    """Test CashLinearAlternationStrategy with synthetic data."""
    config = {
        "linear_asset": "synthetic_linear",
        "cash_asset": "synthetic_cash",
        "order_size": 100,
        "interval": 2,  # Switch every 2 periods
    }

    strategy = CashLinearAlternationStrategy()
    strategy.initialize(config)

    # Combine the datasets for alternation testing
    data = pd.concat([synthetic_linear_data, synthetic_cash_data], axis=1)

    # Run the strategy
    strategy.run(data)

    # Expected outcome (switches between linear and cash every 2 periods)
    expected_results = []
    for i, date in enumerate(data.index):
        if i % 2 == 0:
            expected_results.append(
                {
                    "asset": "synthetic_cash",
                    "price": data.loc[date, "synthetic_cash"],
                    "units": 100,
                }
            )
        else:
            expected_results.append(
                {
                    "asset": "synthetic_linear",
                    "price": data.loc[date, "synthetic_linear"],
                    "units": 100,
                }
            )
    assert strategy.output() == expected_results
