from zipline.utils.cli import maybe_show_progress

def custom_bundle(environ, asset_db_writer, minute_bar_writer, daily_bar_writer,
                  adjustment_writer, calendar, start_session, end_session, cache,
                  show_progress, output_dir):
    """Custom data bundle to ingest CSV data."""
    # Generate CSV from database
    generate_csv_from_db()

    # Ensure the CSV file exists
    if not os.path.exists(csv_temp_path):
        raise FileNotFoundError(f"CSV file not found: {csv_temp_path}")

    print(f"CSV file ready for ingestion: {csv_temp_path}")

    # Load the CSV into a DataFrame
    data = pd.read_csv(csv_temp_path, parse_dates=["date"])

    # Ensure column names are compatible
    data = data.rename(
        columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "sid": "sid",
        }
    )

    # Adjust data types
    data["sid"] = data["sid"].astype("category").cat.codes  # Map tickers to integers

    # Group by 'sid' and write daily bar data
    def data_generator():
        for sid, df in data.groupby("sid"):
            yield sid, df

    # Pass the `show_progress` argument to `maybe_show_progress`
    daily_bar_writer.write(
        data_generator(), show_progress=maybe_show_progress(show_progress)
    )
