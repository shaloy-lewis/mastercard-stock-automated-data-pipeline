"""Clean stock records while preserving source precision and trading dates."""

import logging
import math
import re
from decimal import Decimal, InvalidOperation

import pandas as pd

from .ingestion import (
    DEFAULT_WINDOW_SIZE,
    OFFSET_VARIABLE,
    REQUIRED_COLUMNS,
    InvalidStockRow,
    read_stock_window,
)


PRICE_COLUMNS = ("Open", "High", "Low", "Close")
OPTIONAL_NUMERIC_COLUMNS = ("Dividends", "Stock Splits")
MISSING_VALUES = {"", "null", "none", "nan", "nat", "na", "n/a"}
LOGGER = logging.getLogger(__name__)


def clean_stock_row(row, seen_dates):
    """Return a new cleaned record, or raise InvalidStockRow with its rejection reason.

    All source fields are retained. Numeric values are validated with float() but
    kept as stripped strings to retain their exact source precision. Dates become
    ISO dates without converting to UTC. Only accepted dates enter seen_dates.
    """
    if None in row:
        raise InvalidStockRow("incorrect number of CSV fields")
    missing_columns = REQUIRED_COLUMNS.difference(row)
    if missing_columns:
        raise InvalidStockRow(f"missing required fields: {', '.join(sorted(missing_columns))}")
    if any(value is None for value in row.values()):
        raise InvalidStockRow("missing CSV field values")

    cleaned = {name: str(value).strip() for name, value in row.items()}
    numeric_columns = PRICE_COLUMNS + ("Volume",) + tuple(
        name for name in OPTIONAL_NUMERIC_COLUMNS if name in cleaned
    )
    for name in ("Date",) + numeric_columns:
        if cleaned[name].lower() in MISSING_VALUES:
            raise InvalidStockRow(f"missing value for {name}")

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:[T ].+)?", cleaned["Date"]):
        raise InvalidStockRow("Date must include a full ISO calendar date")
    try:
        timestamp = pd.to_datetime(cleaned["Date"], format="ISO8601", errors="raise")
    except (ValueError, OverflowError) as error:
        raise InvalidStockRow("Date must contain a valid ISO timestamp or date") from error
    if pd.isna(timestamp):
        raise InvalidStockRow("missing or invalid Date")
    trading_date = timestamp.date().isoformat()

    for name in numeric_columns:
        try:
            number = float(cleaned[name])
        except (ValueError, OverflowError) as error:
            raise InvalidStockRow(f"{name} must contain a valid number") from error
        if not math.isfinite(number):
            raise InvalidStockRow(f"{name} must contain a finite number")
        if name in PRICE_COLUMNS and number <= 0:
            raise InvalidStockRow(f"{name} must be positive")

    # Decimal comparisons avoid rounding away small source inconsistencies or
    # fractional volume when float() alone would round to a whole number.
    try:
        prices = {name: Decimal(cleaned[name]) for name in PRICE_COLUMNS}
        volume = Decimal(cleaned["Volume"])
    except InvalidOperation as error:
        raise InvalidStockRow("invalid decimal price or volume") from error
    if not (
        prices["Low"] <= prices["Open"] <= prices["High"]
        and prices["Low"] <= prices["Close"] <= prices["High"]
    ):
        raise InvalidStockRow("Open and Close must fall between Low and High")
    if volume < 0 or volume != volume.to_integral_value():
        raise InvalidStockRow("Volume must be a nonnegative whole number")
    if trading_date in seen_dates:
        raise InvalidStockRow(f"duplicate trading date: {trading_date}")

    cleaned["Date"] = trading_date
    seen_dates.add(trading_date)
    return cleaned


def clean_stock_window(rows):
    """Clean an in-memory window, logging each rejection and accepted/rejected counts."""
    cleaned_rows = []
    seen_dates = set()
    consumed = 0
    for consumed, row in enumerate(rows, start=1):
        try:
            cleaned_rows.append(clean_stock_row(row, seen_dates))
        except InvalidStockRow as error:
            LOGGER.warning("Skipping window row %s: %s", consumed, error)
    LOGGER.info(
        "Cleaned %s source rows: accepted %s, rejected %s",
        consumed, len(cleaned_rows), consumed - len(cleaned_rows),
    )
    return cleaned_rows


def read_clean_stock_window(
    dataset_path=None,
    window_size=DEFAULT_WINDOW_SIZE,
    offset_variable=OFFSET_VARIABLE,
    *,
    variable_store=None,
):
    """Read and clean one source window, then checkpoint all consumed source rows.

    Duplicate dates are checked against valid records in the scanned source
    prefix as well as the current window. Rejections are logged only for the new
    window. Unexpected cleaning or file errors leave the offset unchanged.
    """
    return read_stock_window(
        dataset_path, window_size, offset_variable,
        variable_store=variable_store, row_transform=clean_stock_row,
    )
