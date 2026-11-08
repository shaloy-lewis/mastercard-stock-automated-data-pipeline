"""Price-extrema tests and cleaning-to-analysis window integration tests."""

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from dags.mastercard_pipeline.analysis import analyze_stock_window
from dags.mastercard_pipeline.cleaning import clean_stock_window, read_clean_stock_window
from dags.mastercard_pipeline.ingestion import OFFSET_VARIABLE


def stock_row(day, high, low, close=None):
    return {
        "Date": day,
        "Open": low,
        "High": high,
        "Low": low,
        "Close": close if close is not None else low,
        "Volume": "100",
        "Dividends": "0.0",
        "Stock Splits": "0.0",
    }


class AnalysisTests(unittest.TestCase):
    def test_uses_high_and_low_columns_and_returns_the_associated_rows(self):
        highest = stock_row("2024-11-04", "100", "90", close="91")
        lowest = stock_row("2024-11-05", "99", "80", close="98")
        other = stock_row("2024-11-06", "98", "85", close="86")
        result = analyze_stock_window([highest, lowest, other])
        self.assertEqual(result, {"highest": highest, "lowest": lowest})

    def test_compares_prices_numerically_instead_of_lexicographically(self):
        higher = stock_row("2024-11-04", "100", "10")
        lower = stock_row("2024-11-05", "9", "2")
        result = analyze_stock_window([higher, lower])
        self.assertEqual(result["highest"], higher)
        self.assertEqual(result["lowest"], lower)

    def test_price_differences_beyond_float_precision_are_preserved(self):
        first = stock_row("2024-11-04", "1.000000000000000001", "0.500000000000000002")
        second = stock_row("2024-11-05", "1.000000000000000002", "0.500000000000000001")
        result = analyze_stock_window([first, second])
        self.assertEqual(result["highest"], second)
        self.assertEqual(result["lowest"], second)
        self.assertEqual(result["highest"]["High"], "1.000000000000000002")

    def test_ties_select_earliest_trading_date_from_unsorted_input(self):
        last = stock_row("2024-11-06", "100", "50")
        earliest_high = stock_row("2024-11-04", "100", "60")
        earliest_low = stock_row("2024-11-05", "90", "50")
        result = analyze_stock_window([last, earliest_low, earliest_high])
        self.assertEqual(result["highest"], earliest_high)
        self.assertEqual(result["lowest"], earliest_low)

    def test_empty_window_and_empty_iterator_have_no_extrema(self):
        self.assertEqual(analyze_stock_window([]), {"highest": None, "lowest": None})
        self.assertEqual(analyze_stock_window(iter(())), {"highest": None, "lowest": None})

    def test_missing_window_is_not_mistaken_for_an_empty_window(self):
        with self.assertRaisesRegex(ValueError, "clean window is required"):
            analyze_stock_window(None)

    def test_single_row_is_both_extrema_with_independent_result_copies(self):
        row = stock_row("2024-11-04", "12", "9")
        result = analyze_stock_window([row])
        self.assertEqual(result, {"highest": row, "lowest": row})
        self.assertIsNot(result["highest"], row)
        self.assertIsNot(result["highest"], result["lowest"])
        result["highest"]["High"] = "changed"
        self.assertEqual(row["High"], "12")
        self.assertEqual(result["lowest"]["High"], "12")

    def test_preserves_extra_fields_and_does_not_reorder_or_modify_input(self):
        rows = [stock_row("2024-11-05", "14", "8"), stock_row("2024-11-04", "13", "9")]
        rows[0]["Source Note"] = "extra metadata"
        original = [dict(row) for row in rows]
        result = analyze_stock_window(rows)
        self.assertEqual(rows, original)
        self.assertEqual(result["highest"]["Source Note"], "extra metadata")
        self.assertEqual(result["lowest"]["Stock Splits"], "0.0")
        json.dumps(result, allow_nan=False)

    def test_accepts_a_one_pass_iterator(self):
        rows = [stock_row("2024-11-05", "14", "8"), stock_row("2024-11-04", "13", "9")]
        result = analyze_stock_window(iter(rows))
        self.assertEqual(result, {"highest": rows[0], "lowest": rows[0]})

    def test_missing_analysis_fields_fail_with_the_row_number(self):
        valid = stock_row("2024-11-04", "12", "9")
        for field in ("Date", "High", "Low"):
            invalid = dict(valid)
            del invalid[field]
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "analysis row 2"):
                analyze_stock_window([valid, invalid])

    def test_invalid_prices_fail_instead_of_producing_incorrect_extrema(self):
        for field in ("High", "Low"):
            for value in ("bad", "", "NaN", "Infinity", "-Infinity", "0", "-1", None):
                row = stock_row("2024-11-04", "12", "9")
                row[field] = value
                with self.subTest(field=field, value=value), self.assertRaisesRegex(
                    ValueError, "analysis row 1"
                ):
                    analyze_stock_window([row])

    def test_analysis_requires_cleaned_dates(self):
        for day in ("invalid", "2024-02-30", "2024-11-04 00:00:00-05:00", "20241104", None):
            with self.subTest(day=day), self.assertRaisesRegex(ValueError, "analysis row 1"):
                analyze_stock_window([stock_row(day, "12", "9")])

    def test_inverted_high_low_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Low must not exceed High"):
            analyze_stock_window([stock_row("2024-11-04", "8", "9")])


class AnalysisIntegrationTests(unittest.TestCase):
    def test_rejected_cleaning_rows_cannot_become_extrema(self):
        highest = stock_row("2024-11-04 00:00:00-05:00", "21", "10")
        lowest = stock_row("2024-11-05 00:00:00-05:00", "16", "7")
        invalid = stock_row("2024-11-06 00:00:00-05:00", "1000000", "0.01")
        invalid["Open"] = "-1"
        with self.assertLogs("dags.mastercard_pipeline.cleaning", level="WARNING"):
            clean_rows = clean_stock_window([highest, lowest, invalid])
        result = analyze_stock_window(clean_rows)
        self.assertEqual(result["highest"]["Date"], "2024-11-04")
        self.assertEqual(result["highest"]["High"], "21")
        self.assertEqual(result["lowest"]["Date"], "2024-11-05")
        self.assertEqual(result["lowest"]["Low"], "7")

    def test_each_analysis_uses_its_own_source_window_without_changing_progress(self):
        rows = [
            stock_row("2024-11-04", "100", "70"),
            stock_row("2024-11-05", "90", "60"),
            stock_row("2024-11-06", "5", "3"),
            stock_row("2024-11-07", "4", "2"),
            stock_row("2024-11-08", "8", "6"),
        ]
        state = {}
        variables = Mock()
        variables.get.side_effect = lambda key, default_var: state.get(key, default_var)
        variables.set.side_effect = lambda key, value: state.update({key: value})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stock.csv"
            with path.open("w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=rows[0])
                writer.writeheader()
                writer.writerows(rows)

            first = analyze_stock_window(read_clean_stock_window(path, 2, variable_store=variables))
            self.assertEqual(first, {"highest": rows[0], "lowest": rows[1]})
            self.assertEqual(state[OFFSET_VARIABLE], "2")
            second_window = read_clean_stock_window(path, 2, variable_store=variables)
            writes = variables.set.call_count
            second = analyze_stock_window(second_window)
            self.assertEqual(second, {"highest": rows[2], "lowest": rows[3]})
            self.assertEqual(analyze_stock_window(second_window), second)
            self.assertEqual(variables.set.call_count, writes)
            self.assertEqual(state[OFFSET_VARIABLE], "4")

            final = analyze_stock_window(read_clean_stock_window(path, 2, variable_store=variables))
            self.assertEqual(final, {"highest": rows[4], "lowest": rows[4]})
            self.assertEqual(state[OFFSET_VARIABLE], "5")
            exhausted = analyze_stock_window(read_clean_stock_window(path, 2, variable_store=variables))
            self.assertEqual(exhausted, {"highest": None, "lowest": None})
            self.assertEqual(state[OFFSET_VARIABLE], "5")

    def test_all_rejected_clean_window_has_no_extrema(self):
        invalid = stock_row("2024-11-04", "12", "9")
        invalid["Volume"] = "missing"
        with self.assertLogs("dags.mastercard_pipeline.cleaning", level="WARNING"):
            clean_rows = clean_stock_window([invalid])
        self.assertEqual(analyze_stock_window(clean_rows), {"highest": None, "lowest": None})


if __name__ == "__main__":
    unittest.main()
