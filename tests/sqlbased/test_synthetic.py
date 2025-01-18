# Unit test updates in `test_synthetic.py`
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
from synthetic_pipeline import SyntheticPipeline


class TestSyntheticPipeline(unittest.TestCase):
    def setUp(self):
        self.start_date = "2023-01-03"
        self.end_date = "2023-01-10"
        self.cash_settings = {"start_value": 100.0}
        self.linear_settings = {"start_value": 100.0, "growth_rate": 1.0}
        self.nyse = CustomBusinessDay(calendar=USFederalHolidayCalendar())
        self.expected_dates = pd.date_range(
            self.start_date, self.end_date, freq=self.nyse
        )

    def test_generate_cash(self):
        pipeline = SyntheticPipeline(
            self.start_date,
            self.end_date,
            self.cash_settings,
            self.linear_settings,
            None,
        )

        cash_data = pipeline.generate_cash()

        self.assertEqual(len(cash_data), len(self.expected_dates))
        self.assertTrue((cash_data["value"] == 100.0).all())
        self.assertTrue((cash_data["symbol"] == "synthetic_cash").all())
        self.assertTrue((cash_data["Date"] == self.expected_dates).all())

    def test_generate_linear(self):
        pipeline = SyntheticPipeline(
            self.start_date,
            self.end_date,
            self.cash_settings,
            self.linear_settings,
            None,
        )

        linear_data = pipeline.generate_linear()

        linear_values = pd.Series(
            [100.0 + i for i in range(len(self.expected_dates))],
            index=self.expected_dates,
        )
        linear_data = linear_data.set_index("Date").reindex(self.expected_dates).ffill()

        self.assertTrue(linear_data["value"].equals(linear_values))
        self.assertTrue((linear_data.index == self.expected_dates).all())

    def test_trading_days_validation(self):
        with self.assertRaises(ValueError):
            pipeline = SyntheticPipeline(
                "2023-01-10",
                "2023-01-03",
                self.cash_settings,
                self.linear_settings,
                None,
            )
            pipeline._get_trading_days()

    @patch("synthetic_pipeline.SyntheticPipeline.save_to_db")
    def test_combined_run(self, mock_save_to_db):
        pipeline = SyntheticPipeline(
            self.start_date,
            self.end_date,
            self.cash_settings,
            self.linear_settings,
            None,
        )

        pipeline.run()

        self.assertEqual(mock_save_to_db.call_count, 2)

        args_cash, _ = mock_save_to_db.call_args_list[0]
        args_linear, _ = mock_save_to_db.call_args_list[1]

        cash_data = args_cash[0]
        linear_data = args_linear[0]

        self.assertEqual(len(cash_data), len(self.expected_dates))
        self.assertEqual(len(linear_data), len(self.expected_dates))
        self.assertTrue((cash_data["value"] == 100.0).all())
        self.assertTrue(
            (
                linear_data["value"]
                == [100.0 + i for i in range(len(self.expected_dates))]
            ).all()
        )


if __name__ == "__main__":
    unittest.main()
