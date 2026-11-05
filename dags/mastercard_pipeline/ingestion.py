"""Read consecutive CSV windows and checkpoint progress in an Airflow Variable."""

import csv
import logging
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation


DEFAULT_WINDOW_SIZE = 10
OFFSET_VARIABLE = "mastercard_stock_offset"
DEFAULT_DATASET_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "Mastercard_stock.csv",
)
REQUIRED_COLUMNS = {"Date", "Open", "High", "Low", "Close", "Volume"}
LOGGER = logging.getLogger(__name__)


class InvalidStockRow(ValueError):
    """A data-quality failure that the cleaning stage can report and skip."""


def _validate_row(row, row_number, seen_dates):
    """Validate source records without changing their values or timezone offsets."""
    try:
        if None in row or any(value is None or not value.strip() for value in row.values()):
            raise ValueError("missing values or an incorrect number of fields")

        trading_date = datetime.fromisoformat(row["Date"]).date()
        if trading_date in seen_dates:
            raise ValueError(f"duplicate trading date: {trading_date}")

        prices = {name: Decimal(row[name]) for name in ("Open", "High", "Low", "Close")}
        if any(not value.is_finite() or value <= 0 for value in prices.values()):
            raise ValueError("prices must be finite positive numbers")
        if not (
            prices["Low"] <= prices["Open"] <= prices["High"]
            and prices["Low"] <= prices["Close"] <= prices["High"]
        ):
            raise ValueError("Open and Close must fall between Low and High")

        volume = Decimal(row["Volume"])
        if not volume.is_finite() or volume < 0 or volume != volume.to_integral_value():
            raise ValueError("Volume must be a finite nonnegative whole number")
    except (ValueError, InvalidOperation) as error:
        raise ValueError(f"Invalid CSV data row {row_number}: {error}") from error

    seen_dates.add(trading_date)


def read_stock_window(
    dataset_path=None,
    window_size=DEFAULT_WINDOW_SIZE,
    offset_variable=OFFSET_VARIABLE,
    *,
    variable_store=None,
    row_transform=None,
):
    """Return up to window_size new rows and advance the persisted row offset.

    The offset counts data records, excluding the CSV header. Source values remain
    strings. An optional row_transform(row, seen_dates) can clean each record and
    raise InvalidStockRow to reject it. Rejections consume source positions, but
    unexpected transformation errors prevent checkpointing. Without a transform, Phase 1's
    strict validation is retained. A get/set-compatible variable_store supports
    offline tests without Airflow.
    Calls sharing an offset must run sequentially against the same source file.
    """
    if isinstance(window_size, bool) or not isinstance(window_size, int) or window_size <= 0:
        raise ValueError("window_size must be a positive integer")

    if dataset_path is None:
        dataset_path = os.environ.get("MASTERCARD_DATASET_PATH", DEFAULT_DATASET_PATH)
    if not os.path.isfile(dataset_path):
        raise FileNotFoundError(f"Dataset file not found: {dataset_path}")

    # Resolve Variables during task execution, not when Airflow parses the DAG.
    if variable_store is None:
        from airflow.models import Variable

        variable_store = Variable

    raw_offset = variable_store.get(offset_variable, default_var="0")
    try:
        offset = int(str(raw_offset))
    except ValueError as error:
        raise ValueError(f"{offset_variable} must be a nonnegative integer") from error
    if offset < 0:
        raise ValueError(f"{offset_variable} must be a nonnegative integer")

    rows = []
    seen_dates = set()
    rows_seen = 0
    with open(dataset_path, encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file, strict=True)
        columns = reader.fieldnames
        if not columns or any(not name for name in columns) or len(set(columns)) != len(columns):
            raise ValueError("CSV must have a nonempty header with unique column names")
        missing = REQUIRED_COLUMNS.difference(columns)
        if missing:
            raise ValueError(f"CSV is missing required columns: {', '.join(sorted(missing))}")

        for rows_seen, row in enumerate(reader, start=1):
            # Scan the prefix with the same policy to detect duplicates across windows.
            transformed = None
            if row_transform is None:
                _validate_row(row, rows_seen, seen_dates)
                transformed = row
            else:
                try:
                    transformed = row_transform(row, seen_dates)
                    if not isinstance(transformed, dict):
                        raise TypeError("row_transform must return a record or raise InvalidStockRow")
                except InvalidStockRow as error:
                    if rows_seen > offset:
                        LOGGER.warning("Skipping CSV data row %s: %s", rows_seen, error)
            if rows_seen <= offset:
                continue
            if transformed is not None:
                rows.append(transformed)
            # The window counts source records, including explicitly rejected rows.
            if rows_seen - offset == window_size:
                break

    if rows_seen < offset:
        raise ValueError("Saved offset exceeds the dataset length; check for a replaced or truncated file")
    consumed = max(0, rows_seen - offset)
    if consumed:
        variable_store.set(offset_variable, str(offset + consumed))

    LOGGER.info(
        "Read %s source rows: accepted %s, rejected %s; offset %s -> %s",
        consumed, len(rows), consumed - len(rows), offset, offset + consumed,
    )
    return rows
