def validate_columns(data):
    """Ensure all required columns are present."""
    required_columns = {"sid", "date", "open", "high", "low", "close", "volume"}
    missing_columns = required_columns - set(data.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


# d
def validate_asset_metadata(data):
    """Validate metadata for assets."""
    if len(data["sid"].unique()) != len(data["sid"]):
        raise ValueError(
            "Duplicate sid values found. Ensure all sid values are unique."
        )

    if data.duplicated(subset=["sid", "date"]).any():
        raise ValueError("Duplicate rows found for sid and date.")

    for sid, group in data.groupby("sid"):
        if not group["date"].is_monotonic_increasing:
            raise ValueError(f"Non-contiguous or unsorted dates for sid: {sid}.")

    if (data["volume"] < 0).any():
        raise ValueError("Volume contains invalid negative values.")
