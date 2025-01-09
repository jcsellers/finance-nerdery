
import pandas as pd
import sys
import os

from simulation_utils import process_signals, calculate_portfolio_value



# Print current Python path
print("PYTHONPATH:", sys.path)

# Ensure `src` is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from simulation.utils import process_signals, calculate_portfolio_value



def test_process_signals():
    prices = pd.DataFrame({
        'TEST1': [100, 105, 110],
        'TEST2': [200, 210, 220]
    }, index=pd.date_range('2023-01-01', periods=3))
    signals = [
        {'date': '2023-01-01', 'action': 'buy', 'ticker': 'TEST1', 'weight': 0.6},
        {'date': '2023-01-01', 'action': 'buy', 'ticker': 'TEST2', 'weight': 0.4}
    ]
    allocations, cash_balance, transaction_log = process_signals(signals, 10000, prices)
    assert allocations['TEST1'] == 60
    assert allocations['TEST2'] == 20
    assert cash_balance == 0

def test_calculate_portfolio_value():
    prices = pd.DataFrame({
        'TEST1': [100, 105, 110],
        'TEST2': [200, 210, 220]
    }, index=pd.date_range('2023-01-01', periods=3))
    allocations = {'TEST1': 60, 'TEST2': 20}
    cash_balance = 0
    portfolio_values = calculate_portfolio_value(prices, allocations, cash_balance)
    assert portfolio_values.iloc[0] == 10000
    assert portfolio_values.iloc[-1] == 11000
