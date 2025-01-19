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

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0

    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
    assert all(col in df.columns for col in expected_columns)

    assert pd.api.types.is_datetime64_any_dtype(df.index)

    assert (df["High"] == df["Open"]).all()
    assert (df["Low"] == df["Open"]).all()
    assert (df["Close"] == df["Open"]).all()
    assert (df["Volume"] == 10000).all()


def test_cash_data_generation():
    generator = SyntheticDataGenerator(
        start_date="2023-01-03",
        end_date="2023-01-10",
        ticker="TEST",
        data_type="cash",
        start_value=100,
    )
    df = generator.generate()

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0

    assert (df["Open"] == 100).all()
    assert (df["High"] == df["Open"]).all()
    assert (df["Low"] == df["Open"]).all()
    assert (df["Close"] == df["Open"]).all()
    assert (df["Volume"] == 10000).all()


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


def test_missing_data_simulation():
    # This test is no longer relevant as the generator is now deterministic
    pass


# Run tests
if __name__ == "__main__":
    pytest.main()
