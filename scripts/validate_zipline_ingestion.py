import pandas as pd
from zipline.data import bundles

# Load the custom bundle
bundle_name = "custom_csv"
custom_bundle = bundles.load(bundle_name)

# Access the asset metadata
assets = custom_bundle.asset_finder.retrieve_all()
print("Ingested assets:")
for asset in assets:
    print(asset)

# Load and inspect ingested data
calendar = custom_bundle.trading_calendar.sessions_in_range(
    custom_bundle.equity_daily_bar_reader.first_trading_day,
    custom_bundle.equity_daily_bar_reader.last_trading_day,
)

reader = custom_bundle.equity_daily_bar_reader
for asset in assets:
    data = reader.load_raw_arrays(
        ["open", "high", "low", "close", "volume"], calendar, [asset.sid]
    )
    df = pd.DataFrame(
        data, index=calendar, columns=["open", "high", "low", "close", "volume"]
    )
    print(f"Data for {asset.symbol}:")
    print(df.head())
