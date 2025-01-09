
def buy_and_hold_signals(portfolio, start_date, end_date):
    return [
        {'date': start_date, 'action': 'buy', 'ticker': ticker, 'weight': weight}
        for ticker, weight in portfolio.items()
    ]
