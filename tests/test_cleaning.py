"""Data-quality and source-window checkpoint tests for Phase 2."""

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from dags.mastercard_pipeline.cleaning import (
    clean_stock_row,
    clean_stock_window,
    read_clean_stock_window,
)
from dags.mastercard_pipeline.ingestion import OFFSET_VARIABLE, InvalidStockRow


VALID_ROW = {
    "Date": "2024-11-04 00:00:00-05:00",
    "Open": "10.1234567890123456789",
    "High": "12",
    "Low": "9",
    "Close": "11",
    "Volume": "100",
    "Dividends": "0.0",
    "Stock Splits": "0.0",
}


class CleaningTests(unittest.TestCase):
    def test_strips_all_values_normalizes_date_and_preserves_source_precision(self):
        original = {key: f" \t{value} \n" for key, value in VALID_ROW.items()}
        original["Source Note"] = "  additional metadata  "
        before = dict(original)
        result = clean_stock_row(original, set())
        self.assertEqual(result["Date"], "2024-11-04")
        self.assertEqual(result["Open"], "10.1234567890123456789")
        self.assertEqual(result["Source Note"], "additional metadata")
        self.assertEqual(set(result), set(original))
        self.assertEqual(original, before)
        json.dumps(result, allow_nan=False)

    def test_timezone_offsets_do_not_shift_the_trading_date(self):
        for date in (
            "2024-11-04 23:30:00-05:00",
            "2024-11-04 00:30:00+14:00",
            "2024-11-04T00:00:00Z",
            "2024-11-04",
        ):
            with self.subTest(date=date):
                self.assertEqual(
                    clean_stock_row(dict(VALID_ROW, Date=date), set())["Date"],
                    "2024-11-04",
                )

    def test_mixed_daylight_saving_offsets_are_parsed_individually(self):
        rows = [dict(VALID_ROW, Date="2024-11-01 00:00:00-04:00"), VALID_ROW]
        self.assertEqual(
            [row["Date"] for row in clean_stock_window(rows)],
            ["2024-11-01", "2024-11-04"],
        )

    def test_missing_required_values_are_rejected(self):
        for column in VALID_ROW:
            for value in (None, "", " \t ", "NULL", "None", "NaN", "N/A"):
                with self.subTest(column=column, value=value):
                    row = dict(VALID_ROW)
                    row[column] = value
                    with self.assertRaises(InvalidStockRow):
                        clean_stock_row(row, set())

    def test_missing_required_column_is_rejected(self):
        row = dict(VALID_ROW)
        del row["Volume"]
        with self.assertRaisesRegex(InvalidStockRow, "missing required fields: Volume"):
            clean_stock_row(row, set())

    def test_invalid_or_incomplete_dates_are_rejected(self):
        for date in ("not-a-date", "2024-02-30", "2024", "2024-11", "11/04/2024", "12345"):
            with self.subTest(date=date), self.assertRaises(InvalidStockRow):
                clean_stock_row(dict(VALID_ROW, Date=date), set())

    def test_non_numeric_and_non_finite_values_are_rejected(self):
        for column in ("Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"):
            for value in ("invalid", "Infinity", "-Infinity", "1e999"):
                row = dict(VALID_ROW)
                row[column] = value
                with self.subTest(column=column, value=value), self.assertRaises(InvalidStockRow):
                    clean_stock_row(row, set())

    def test_zero_and_negative_prices_are_rejected(self):
        for column in ("Open", "High", "Low", "Close"):
            for value in ("0", "-1"):
                row = dict(VALID_ROW)
                row[column] = value
                with self.subTest(column=column, value=value), self.assertRaisesRegex(
                    InvalidStockRow, "must be positive"
                ):
                    clean_stock_row(row, set())

    def test_inconsistent_price_ranges_are_rejected_without_rounding(self):
        for values in (
            {"Open": "13"}, {"Close": "8"}, {"Low": "13"},
            {"Open": "12.000000000000000001"},
        ):
            with self.subTest(values=values), self.assertRaisesRegex(
                InvalidStockRow, "between Low and High"
            ):
                clean_stock_row(dict(VALID_ROW, **values), set())

    def test_volume_requires_an_exact_nonnegative_whole_number(self):
        for value in ("-1", "1.5", "9007199254740993.1"):
            with self.subTest(value=value), self.assertRaisesRegex(
                InvalidStockRow, "nonnegative whole number"
            ):
                clean_stock_row(dict(VALID_ROW, Volume=value), set())
        for value in ("0", "100.0", "9007199254740993"):
            with self.subTest(value=value):
                result = clean_stock_row(dict(VALID_ROW, Volume=value), set())
                self.assertEqual(result["Volume"], value)

    def test_optional_source_columns_are_preserved_and_can_be_absent(self):
        row = {key: value for key, value in VALID_ROW.items() if key not in ("Dividends", "Stock Splits")}
        row["Note"] = "  "
        result = clean_stock_row(row, set())
        self.assertEqual(result["Note"], "")
        self.assertNotIn("Dividends", result)
        self.assertNotIn("Stock Splits", result)

    def test_rejections_are_logged_with_reasons_and_summary(self):
        rows = [VALID_ROW, dict(VALID_ROW, Date="2024-11-05", Open="-1")]
        with self.assertLogs("dags.mastercard_pipeline.cleaning", level="INFO") as captured:
            cleaned = clean_stock_window(rows)
        self.assertEqual(len(cleaned), 1)
        self.assertTrue(any("Skipping window row 2: Open must be positive" in line for line in captured.output))
        self.assertTrue(any("accepted 1, rejected 1" in line for line in captured.output))

    def test_duplicate_dates_are_reported_and_invalid_rows_do_not_claim_dates(self):
        invalid = dict(VALID_ROW, Open="-1")
        duplicate = dict(VALID_ROW, Date="2024-11-04 15:00:00-05:00")
        with self.assertLogs("dags.mastercard_pipeline.cleaning", level="WARNING") as captured:
            cleaned = clean_stock_window([invalid, VALID_ROW, duplicate])
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["Open"], VALID_ROW["Open"])
        self.assertTrue(any("duplicate trading date" in line for line in captured.output))

    def test_malformed_row_shapes_are_rejected(self):
        for row in (dict(VALID_ROW, **{"Volume": None}), {**VALID_ROW, None: ["extra"]}):
            with self.subTest(row=row), self.assertRaises(InvalidStockRow):
                clean_stock_row(row, set())

    def test_empty_window_returns_an_empty_list(self):
        self.assertEqual(clean_stock_window([]), [])


class CleaningWindowTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "stock.csv"
        self.state = {}
        self.variables = Mock()
        self.variables.get.side_effect = lambda key, default_var: self.state.get(key, default_var)
        self.variables.set.side_effect = lambda key, value: self.state.update({key: value})
        self.rows = [dict(VALID_ROW, Date=f"2024-11-{day:02d}") for day in range(4, 9)]

    def write(self, rows):
        with self.path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=VALID_ROW.keys())
            writer.writeheader()
            writer.writerows(rows)

    def run_window(self):
        return read_clean_stock_window(self.path, 2, variable_store=self.variables)

    def test_offsets_count_raw_records_including_rejections_and_partial_eof(self):
        self.rows[1]["Open"] = "-1"
        self.rows[2]["Date"] = "invalid"
        self.rows[0]["Date"] = " 2024-11-04 00:00:00-05:00 "
        self.write(self.rows)
        with self.assertLogs("dags.mastercard_pipeline.ingestion", level="INFO") as captured:
            self.assertEqual([row["Date"] for row in self.run_window()], ["2024-11-04"])
            self.assertEqual(self.state[OFFSET_VARIABLE], "2")
            self.assertEqual([row["Date"] for row in self.run_window()], ["2024-11-07"])
            self.assertEqual(self.state[OFFSET_VARIABLE], "4")
            self.assertEqual([row["Date"] for row in self.run_window()], ["2024-11-08"])
            self.assertEqual(self.state[OFFSET_VARIABLE], "5")
            self.assertEqual(self.run_window(), [])
            self.assertEqual(self.state[OFFSET_VARIABLE], "5")
        rejected = [line for line in captured.output if "Skipping CSV data row" in line]
        self.assertEqual(len(rejected), 2, "Previously consumed rejections must not be logged again")
        self.assertTrue(any("CSV data row 2" in line for line in rejected))
        self.assertTrue(any("CSV data row 3" in line for line in rejected))

    def test_all_invalid_window_advances_and_next_window_can_still_be_read(self):
        self.rows[0]["Open"] = "-1"
        self.rows[1]["Volume"] = "not-a-number"
        self.write(self.rows)
        with self.assertLogs("dags.mastercard_pipeline.ingestion", level="WARNING"):
            self.assertEqual(self.run_window(), [])
        self.assertEqual(self.state[OFFSET_VARIABLE], "2")
        self.assertEqual(len(self.run_window()), 2)
        self.assertEqual(self.state[OFFSET_VARIABLE], "4")

    def test_duplicate_dates_across_source_windows_are_skipped(self):
        self.rows[2]["Date"] = " 2024-11-04 15:00:00-05:00 "
        self.write(self.rows)
        self.assertEqual(len(self.run_window()), 2)
        with self.assertLogs("dags.mastercard_pipeline.ingestion", level="WARNING") as captured:
            self.assertEqual([row["Date"] for row in self.run_window()], ["2024-11-07"])
        self.assertEqual(self.state[OFFSET_VARIABLE], "4")
        self.assertTrue(any("CSV data row 3: duplicate trading date" in line for line in captured.output))

    def test_unexpected_cleaning_error_does_not_checkpoint_a_partial_window(self):
        self.write(self.rows)
        calls = 0

        def fail_second_record(row, seen_dates):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("unexpected cleaner failure")
            return clean_stock_row(row, seen_dates)

        with patch("dags.mastercard_pipeline.cleaning.clean_stock_row", side_effect=fail_second_record):
            with self.assertRaisesRegex(RuntimeError, "unexpected cleaner failure"):
                self.run_window()
        self.variables.set.assert_not_called()

    def test_unrecoverable_csv_syntax_does_not_checkpoint(self):
        self.write(self.rows[:1])
        with self.path.open("a", encoding="utf-8", newline="") as file:
            file.write('"unterminated\n')
        with self.assertRaises(csv.Error):
            self.run_window()
        self.variables.set.assert_not_called()

    def test_invalid_cleaner_result_fails_instead_of_silently_dropping_rows(self):
        self.write(self.rows)
        with patch("dags.mastercard_pipeline.cleaning.clean_stock_row", return_value=None):
            with self.assertRaisesRegex(TypeError, "must return a record"):
                self.run_window()
        self.variables.set.assert_not_called()


if __name__ == "__main__":
    unittest.main()
