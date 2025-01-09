
def process_signals(signals, initial_investment, prices):
    allocations = {}
    cash_balance = initial_investment
    transaction_log = []

    for signal in signals:
        date = signal['date']
        action = signal['action']
        ticker = signal['ticker']
        weight = signal.get('weight', 0)

        if action == 'buy':
            price = prices.at[date, ticker]
            allocation = (weight * initial_investment) / price
            allocations[ticker] = allocations.get(ticker, 0) + allocation
            transaction_log.append({'date': date, 'action': 'buy', 'ticker': ticker, 'price': price, 'shares': allocation})
            cash_balance -= allocation * price

    return allocations, cash_balance, transaction_log


def calculate_portfolio_value(prices, allocations, cash_balance):
    return prices.mul(pd.Series(allocations), axis=1).sum(axis=1) + cash_balance
