"""Write a window's highest and lowest stock records to a summary CSV."""

import csv
import logging
import os
import tempfile
from datetime import date
from pathlib import Path


LOGGER = logging.getLogger(__name__)
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "output"
OUTPUT_COLUMNS = ("type", "date", "open", "high_or_low", "close", "volume")


def export_stock_results(results, rows, output_dir=None):
    """Export two summary rows and return the path, or None for an empty window.

    The filename covers all accepted trading dates, not just the extrema dates.
    Input comes from cleaning and analysis; numeric strings are written unchanged.
    A repeat export of the same date range atomically replaces the existing file,
    so retries neither append duplicate rows nor expose partially written CSVs.
    This function does not read the source CSV or change the ingestion offset.
    """
    if rows is None:
        raise ValueError("A clean window is required for export")
    window = list(rows)
    if not isinstance(results, dict) or not {"highest", "lowest"} <= results.keys():
        raise ValueError("Analysis results must include highest and lowest")

    if not window:
        if results["highest"] is not None or results["lowest"] is not None:
            raise ValueError("An empty window cannot have price extrema")
        LOGGER.info("Clean window is empty; no results CSV written")
        return None

    dates = []
    for row_number, row in enumerate(window, start=1):
        try:
            trading_date = date.fromisoformat(row["Date"])
            if trading_date.isoformat() != row["Date"]:
                raise ValueError("Date must be a cleaned YYYY-MM-DD date")
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"Invalid export window row {row_number}: {error}") from error
        dates.append(trading_date)
    dates.sort()

    summaries = []
    for key, label, price_column in (
        ("highest", "HIGHEST", "High"),
        ("lowest", "LOWEST", "Low"),
    ):
        record = results[key]
        if not isinstance(record, dict) or record not in window:
            raise ValueError(f"The {key} result must be a record from the clean window")
        source_columns = ("Date", "Open", price_column, "Close", "Volume")
        for column in source_columns:
            if record.get(column) is None or not str(record[column]).strip():
                raise ValueError(f"The {key} result is missing {column}")
        summaries.append({
            "type": label,
            "date": record["Date"],
            "open": record["Open"],
            "high_or_low": record[price_column],
            "close": record["Close"],
            "volume": record["Volume"],
        })

    target_dir = Path(
        output_dir if output_dir is not None
        else os.environ.get("MASTERCARD_OUTPUT_DIR") or DEFAULT_OUTPUT_DIR
    ).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    output_path = target_dir / f"{dates[0].isoformat()}_to_{dates[-1].isoformat()}.csv"
    temporary_path = None
    try:
        # Write beside the destination so os.replace stays on the same filesystem.
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", dir=target_dir,
            prefix=f".{output_path.stem}_", suffix=".tmp", delete=False,
        ) as file:
            temporary_path = Path(file.name)
            writer = csv.DictWriter(file, fieldnames=OUTPUT_COLUMNS)
            writer.writeheader()
            writer.writerows(summaries)
        os.replace(temporary_path, output_path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    LOGGER.info("Exported %s clean rows as two summary records to %s", len(window), output_path)
    return str(output_path)
