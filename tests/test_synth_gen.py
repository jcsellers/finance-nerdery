import pandas as pd
import pytest

from synthetic_data_generator import SyntheticDataGenerator


def test_linear_data_generation():
    generator = SyntheticDataGenerator(
        start_date="2023-01-03",
        end_date="2023-01-10",
        ticker="TEST",
        data_type="linear",
        start_value=100,
        growth_rate=1,
    )
    df = generator.generate()

    # Validate DataFrame structure
    assert isinstance(df, pd.DataFrame), "Output is not a DataFrame."
    assert len(df) > 0, "Generated data is empty."

    # Validate schema
    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
    assert all(
        col in df.columns for col in expected_columns
    ), "Missing columns in generated data."

    # Validate index type
    assert pd.api.types.is_datetime64_any_dtype(df.index), "Index is not datetime."

    # Validate OHLCV consistency
    assert (df["High"] == df["Open"]).all(), "High values do not match Open."
    assert (df["Low"] == df["Open"]).all(), "Low values do not match Open."
    assert (df["Close"] == df["Open"]).all(), "Close values do not match Open."
    assert (df["Volume"] == 10000).all(), "Volume values are incorrect."


def test_cash_data_generation():
    generator = SyntheticDataGenerator(
        start_date="2023-01-03",
        end_date="2023-01-10",
        ticker="TEST",
        data_type="cash",
        start_value=100,
    )
    df = generator.generate()

    # Validate DataFrame structure
    assert isinstance(df, pd.DataFrame), "Output is not a DataFrame."
    assert len(df) > 0, "Generated data is empty."

    # Validate schema
    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
    assert all(
        col in df.columns for col in expected_columns
    ), "Missing columns in generated data."

    # Validate OHLCV consistency
    assert (df["Open"] == 100).all(), "Open values do not match expected value."
    assert (df["High"] == df["Open"]).all(), "High values do not match Open."
    assert (df["Low"] == df["Open"]).all(), "Low values do not match Open."
    assert (df["Close"] == df["Open"]).all(), "Close values do not match Open."
    assert (df["Volume"] == 10000).all(), "Volume values are incorrect."


def test_no_trading_days():
    generator = SyntheticDataGenerator(
        start_date="2023-01-01",  # Non-trading day (Sunday)
        end_date="2023-01-02",  # Another non-trading day (Monday holiday)
        ticker="TEST",
        data_type="linear",
        start_value=100,
        growth_rate=1,
    )
    with pytest.raises(
        ValueError, match="No valid trading days in the specified range."
    ):
        generator.generate()


def test_invalid_data_type():
    with pytest.raises(
        ValueError, match="Unsupported data type. Use 'linear' or 'cash'."
    ):
        generator = SyntheticDataGenerator(
            start_date="2023-01-03",
            end_date="2023-01-10",
            ticker="TEST",
            data_type="invalid",
            start_value=100,
        )
        generator.generate()


def test_valid_trading_days():
    generator = SyntheticDataGenerator(
        start_date="2023-01-03",
        end_date="2023-01-05",
        ticker="TEST",
        data_type="linear",
        start_value=100,
        growth_rate=1,
    )
    df = generator.generate()

    # Validate that generated dates align with NYSE trading calendar
    expected_dates = pd.to_datetime(["2023-01-03", "2023-01-04", "2023-01-05"])
    assert list(df.index) == list(expected_dates), "Trading days mismatch."


if __name__ == "__main__":
    pytest.main()
