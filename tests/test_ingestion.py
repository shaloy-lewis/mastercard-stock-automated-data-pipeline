"""Window and checkpoint tests using synthetic data and an in-memory Variable."""

import csv
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch

from dags.mastercard_pipeline.ingestion import OFFSET_VARIABLE, read_stock_window


COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"]


class IngestionTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "stocks.csv"
        self.values = {}
        self.variables = Mock()
        self.variables.get.side_effect = lambda key, default_var: self.values.get(key, default_var)
        self.variables.set.side_effect = lambda key, value: self.values.update({key: value})
        dates = [
            "2024-11-01 00:00:00-04:00",
            "2024-11-04 00:00:00-05:00",
            "2024-11-05 00:00:00-05:00",
            "2024-11-06 00:00:00-05:00",
            "2024-11-07 00:00:00-05:00",
        ]
        self.rows = [
            dict(zip(COLUMNS, [date, "10.123456789", "12", "9", "11", "100", "0.0", "0.0"]))
            for date in dates
        ]
        self.write_rows(self.rows)

    def write_rows(self, rows, columns=COLUMNS):
        with self.path.open("w", encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)

    def run_window(self, window_size=2, **kwargs):
        return read_stock_window(
            self.path, window_size, variable_store=self.variables, **kwargs
        )

    def test_consecutive_runs_return_next_windows_and_partial_final_window(self):
        self.assertEqual(self.run_window(), self.rows[:2])
        self.assertEqual(self.values[OFFSET_VARIABLE], "2")
        self.assertEqual(self.run_window(), self.rows[2:4])
        self.assertEqual(self.values[OFFSET_VARIABLE], "4")
        self.assertEqual(self.run_window(), self.rows[4:])
        self.assertEqual(self.values[OFFSET_VARIABLE], "5")
        writes = self.variables.set.call_count
        self.assertEqual(self.run_window(), [])
        self.assertEqual(self.run_window(), [])
        self.assertEqual(self.variables.set.call_count, writes)
        self.assertEqual(self.values[OFFSET_VARIABLE], "5")

    def test_existing_offset_and_changed_window_size(self):
        self.values[OFFSET_VARIABLE] = "1"
        self.assertEqual(self.run_window(window_size=3), self.rows[1:4])
        self.assertEqual(self.run_window(window_size=1), self.rows[4:])

    def test_header_only_file_returns_empty_window_without_advancing(self):
        self.write_rows([])
        self.assertEqual(self.run_window(), [])
        self.variables.set.assert_not_called()

    def test_missing_dataset_does_not_access_variables(self):
        with self.assertRaises(FileNotFoundError):
            read_stock_window(self.path.parent / "missing.csv", variable_store=self.variables)
        self.variables.get.assert_not_called()
        self.variables.set.assert_not_called()

    def test_invalid_window_sizes_do_not_change_offset(self):
        for size in (0, -1, 1.5, "2", True, None):
            with self.subTest(size=size), self.assertRaises(ValueError):
                self.run_window(window_size=size)
        self.variables.set.assert_not_called()

    def test_invalid_saved_offsets_are_rejected(self):
        for offset in ("-1", "abc", "1.5", "", None):
            self.values[OFFSET_VARIABLE] = offset
            with self.subTest(offset=offset), self.assertRaises(ValueError):
                self.run_window()
        self.variables.set.assert_not_called()

    def test_offset_beyond_end_does_not_reset_or_advance(self):
        self.values[OFFSET_VARIABLE] = "6"
        with self.assertRaisesRegex(ValueError, "exceeds the dataset length"):
            self.run_window()
        self.assertEqual(self.values[OFFSET_VARIABLE], "6")
        self.variables.set.assert_not_called()

    def test_missing_required_column_and_empty_file(self):
        self.path.write_text("Date,Open\n2024-11-01,10\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "missing required columns"):
            self.run_window()
        self.path.write_text("", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "header"):
            self.run_window()
        self.variables.set.assert_not_called()

    def test_duplicate_header_is_rejected(self):
        self.path.write_text(
            "Date,Open,High,Low,Close,Volume,Open\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "unique column names"):
            self.run_window()
        self.variables.set.assert_not_called()

    def test_invalid_row_does_not_partially_advance_window(self):
        for column, value in (
            ("Date", "not-a-date"), ("Open", ""), ("High", "not-a-number"),
            ("Low", "-1"), ("Close", "20"), ("Open", "NaN"),
            ("High", "Infinity"), ("Volume", "-1"), ("Volume", "1.5"),
        ):
            with self.subTest(column=column, value=value):
                rows = [dict(row) for row in self.rows]
                rows[1][column] = value
                self.write_rows(rows)
                with self.assertRaisesRegex(ValueError, "data row 2"):
                    self.run_window()
                self.variables.set.assert_not_called()

    def test_duplicate_date_across_windows_is_rejected(self):
        self.run_window()
        self.rows[2]["Date"] = self.rows[0]["Date"]
        self.write_rows(self.rows)
        with self.assertRaisesRegex(ValueError, "duplicate trading date"):
            self.run_window()
        self.assertEqual(self.values[OFFSET_VARIABLE], "2")

    def test_malformed_csv_does_not_advance_offset(self):
        malformed_rows = (
            "2024-11-01,10,12,9,11,100,0\n",
            "2024-11-01,10,12,9,11,100,0,0,extra\n",
            '"unterminated\n',
        )
        for row in malformed_rows:
            self.path.write_text(
                ",".join(COLUMNS) + "\n" + row, encoding="utf-8"
            )
            with self.subTest(row=row), self.assertRaises((ValueError, csv.Error)):
                self.run_window()
        self.variables.set.assert_not_called()

    def test_multiline_extra_fields_count_as_one_record_and_are_preserved(self):
        columns = COLUMNS + ["Note"]
        rows = [dict(row, Note="quoted, value\nwith newline") for row in self.rows]
        self.write_rows(rows, columns=columns)
        self.assertEqual(self.run_window(), rows[:2])
        self.assertEqual(self.run_window(), rows[2:4])

    def test_environment_path_is_resolved_when_called(self):
        with patch.dict(os.environ, {"MASTERCARD_DATASET_PATH": str(self.path)}):
            result = read_stock_window(window_size=1, variable_store=self.variables)
        self.assertEqual(result, self.rows[:1])

    def test_custom_variable_key_is_used(self):
        self.run_window(offset_variable="another_dataset_offset")
        self.assertEqual(self.values, {"another_dataset_offset": "2"})

    def test_default_store_uses_airflow_variable_api(self):
        models = ModuleType("airflow.models")
        models.Variable = self.variables
        with patch.dict(sys.modules, {"airflow": ModuleType("airflow"), "airflow.models": models}):
            result = read_stock_window(self.path, window_size=2)
        self.assertEqual(result, self.rows[:2])
        self.variables.get.assert_called_once_with(OFFSET_VARIABLE, default_var="0")
        self.variables.set.assert_called_once_with(OFFSET_VARIABLE, "2")

    def test_variable_failure_is_propagated(self):
        self.variables.set.side_effect = RuntimeError("Variable store unavailable")
        with self.assertRaisesRegex(RuntimeError, "Variable store unavailable"):
            self.run_window()
        self.assertEqual(self.values, {})


if __name__ == "__main__":
    unittest.main()
