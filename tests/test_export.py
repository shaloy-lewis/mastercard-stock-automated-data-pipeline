"""CSV export and full window pipeline tests without an Airflow dependency."""

import csv
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from dags.mastercard_pipeline.analysis import analyze_stock_window
from dags.mastercard_pipeline.cleaning import read_clean_stock_window
from dags.mastercard_pipeline.export import export_stock_results
from dags.mastercard_pipeline.ingestion import OFFSET_VARIABLE


def stock_row(day, high="12.000000000000000002", low="9.000000000000000001"):
    return {
        "Date": day, "Open": "10.0100", "High": high, "Low": low,
        "Close": "11.0000", "Volume": "15344000", "Dividends": "0.0",
        "Stock Splits": "0.0",
    }


def read_csv(path):
    with Path(path).open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        return reader.fieldnames, list(reader)


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.output = Path(self.directory.name).resolve() / "output"
        self.rows = [
            stock_row("2024-11-04"), stock_row("2024-11-05", high="13.123456789012345678"),
            stock_row("2024-11-06", low="8.123456789012345678"), stock_row("2024-11-07"),
        ]

    def export(self, rows=None, results=None):
        rows = self.rows if rows is None else rows
        results = analyze_stock_window(rows) if results is None else results
        return export_stock_results(results, rows, self.output)

    def test_exact_csv_schema_values_precision_and_full_window_filename(self):
        path = self.export()
        self.assertEqual(Path(path), self.output / "2024-11-04_to_2024-11-07.csv")
        columns, records = read_csv(path)
        self.assertEqual(columns, ["type", "date", "open", "high_or_low", "close", "volume"])
        self.assertEqual(records, [
            {"type": "HIGHEST", "date": "2024-11-05", "open": "10.0100",
             "high_or_low": "13.123456789012345678", "close": "11.0000", "volume": "15344000"},
            {"type": "LOWEST", "date": "2024-11-06", "open": "10.0100",
             "high_or_low": "8.123456789012345678", "close": "11.0000", "volume": "15344000"},
        ])

    def test_unsorted_iterator_uses_earliest_and_latest_dates_without_mutation(self):
        rows = list(reversed(self.rows))
        results = analyze_stock_window(rows)
        original = [dict(row) for row in rows]
        original_results = {key: dict(row) for key, row in results.items()}
        path = export_stock_results(results, iter(rows), self.output)
        self.assertEqual(Path(path).name, "2024-11-04_to_2024-11-07.csv")
        self.assertEqual(rows, original)
        self.assertEqual(results, original_results)

    def test_single_row_produces_two_labeled_records(self):
        path = self.export(rows=self.rows[:1])
        self.assertEqual(Path(path).name, "2024-11-04_to_2024-11-04.csv")
        records = read_csv(path)[1]
        self.assertEqual([row["type"] for row in records], ["HIGHEST", "LOWEST"])
        self.assertEqual([row["date"] for row in records], ["2024-11-04"] * 2)

    def test_empty_window_does_not_create_output_directory_or_csv(self):
        with self.assertLogs("dags.mastercard_pipeline.export", level="INFO") as logs:
            self.assertIsNone(self.export(rows=[]))
        self.assertIn("no results CSV written", logs.output[0])
        self.assertFalse(self.output.exists())

    def test_repeated_export_replaces_file_without_duplicate_rows(self):
        path = self.export()
        expected = Path(path).read_bytes()
        Path(path).write_text("stale output", encoding="utf-8")
        self.assertEqual(self.export(), path)
        self.assertEqual(Path(path).read_bytes(), expected)
        self.assertEqual(list(self.output.iterdir()), [Path(path)])

    def test_configured_output_directory(self):
        results = analyze_stock_window(self.rows)
        with patch.dict(os.environ, {"MASTERCARD_OUTPUT_DIR": str(self.output)}):
            path = export_stock_results(results, self.rows)
        self.assertEqual(Path(path).parent, self.output)

    def test_explicit_output_directory_overrides_environment(self):
        with patch.dict(os.environ, {"MASTERCARD_OUTPUT_DIR": str(self.output / "unused")}):
            self.export()
        self.assertFalse((self.output / "unused").exists())

    def test_missing_or_mismatched_analysis_fails_before_creating_files(self):
        for results in (
            None, {}, {"highest": self.rows[0]}, {"highest": None, "lowest": None},
            {"highest": stock_row("2024-11-08"), "lowest": self.rows[0]},
        ):
            with self.subTest(results=results), self.assertRaises(ValueError):
                export_stock_results(results, self.rows, self.output)
        with self.assertRaisesRegex(ValueError, "empty window"):
            export_stock_results(analyze_stock_window(self.rows), [], self.output)
        with self.assertRaisesRegex(ValueError, "clean window is required"):
            export_stock_results({}, None, self.output)
        self.assertFalse(self.output.exists())

    def test_unnormalized_or_invalid_dates_cannot_be_used_in_filenames(self):
        for day in ("../wrong", "2024-02-30", "20241104", "2024-11-04T00:00:00-05:00", None):
            row = stock_row(day)
            with self.subTest(day=day), self.assertRaisesRegex(ValueError, "window row 1"):
                export_stock_results({"highest": row, "lowest": row}, [row], self.output)
        self.assertFalse(self.output.exists())

    def test_missing_summary_fields_fail_without_writing_an_incomplete_csv(self):
        for column in ("Open", "Close", "Volume"):
            for value in (None, " "):
                row = stock_row("2024-11-04")
                row[column] = value
                with self.subTest(column=column, value=value), self.assertRaisesRegex(
                    ValueError, f"missing {column}"
                ):
                    self.export(rows=[row])
        self.assertFalse(self.output.exists())

    def test_failed_replace_keeps_previous_csv_and_removes_temporary_file(self):
        path = Path(self.export())
        previous = path.read_bytes()
        with patch("dags.mastercard_pipeline.export.os.replace", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                self.export()
        self.assertEqual(path.read_bytes(), previous)
        self.assertEqual(list(self.output.iterdir()), [path])

    def test_failed_write_does_not_publish_a_partial_file(self):
        with patch("dags.mastercard_pipeline.export.csv.DictWriter.writerows", side_effect=OSError("disk full")):
            with self.assertRaisesRegex(OSError, "disk full"):
                self.export()
        self.assertEqual(list(self.output.iterdir()), [])


class ExportIntegrationTests(unittest.TestCase):
    def test_successive_windows_rejections_repeat_export_and_eof(self):
        rows = [
            stock_row("2024-11-04T23:30:00-05:00"),
            stock_row("2024-11-05", high="-1"),
            stock_row("2024-11-06", high="13"),
            stock_row("2024-11-07", low="8"),
            stock_row("2024-11-07", high="999"),  # Later duplicate is rejected.
        ]
        state = {}
        variables = Mock()
        variables.get.side_effect = lambda key, default_var: state.get(key, default_var)
        variables.set.side_effect = lambda key, value: state.update({key: value})
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "stock.csv"
            output = Path(directory) / "output"
            with source.open("w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=rows[0])
                writer.writeheader()
                writer.writerows(rows)
            original = source.read_bytes()

            def run_window():
                window = read_clean_stock_window(source, 2, variable_store=variables)
                result = analyze_stock_window(window)
                writes = variables.set.call_count
                path = export_stock_results(result, window, output)
                self.assertEqual(export_stock_results(result, window, output), path)
                self.assertEqual(variables.set.call_count, writes)
                return path

            with self.assertLogs("dags.mastercard_pipeline.ingestion", level="WARNING"):
                first = run_window()
            self.assertEqual(Path(first).name, "2024-11-04_to_2024-11-04.csv")
            self.assertEqual(state[OFFSET_VARIABLE], "2")
            second = run_window()
            self.assertEqual(Path(second).name, "2024-11-06_to_2024-11-07.csv")
            self.assertEqual(state[OFFSET_VARIABLE], "4")
            self.assertEqual([row["date"] for row in read_csv(second)[1]], ["2024-11-06", "2024-11-07"])
            with self.assertLogs("dags.mastercard_pipeline.ingestion", level="WARNING"):
                self.assertIsNone(run_window())  # All-rejected final partial window.
            self.assertEqual(state[OFFSET_VARIABLE], "5")
            self.assertIsNone(run_window())  # EOF leaves the offset and prior exports intact.
            self.assertEqual(state[OFFSET_VARIABLE], "5")
            self.assertEqual(len(list(output.glob("*.csv"))), 2)
            self.assertEqual(source.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
