"""Find the trading days containing the highest High and lowest Low in a window."""

import logging
from datetime import date
from decimal import Decimal, InvalidOperation


LOGGER = logging.getLogger(__name__)


def analyze_stock_window(rows):
    """Return full source records under 'highest' and 'lowest', or None when empty.

    Input is the Phase 2 clean window. Decimal comparisons retain source price
    precision; no numeric values are rounded or converted in the returned rows.
    Ties select the earliest trading date, independently of input order. Input
    records and their order are left unchanged, and results are separate copies.
    Malformed analysis input fails explicitly instead of silently losing records.
    """
    if rows is None:
        raise ValueError("A clean window is required; received None instead of rows")

    prepared = []
    for row_number, row in enumerate(rows, start=1):
        try:
            trading_date = date.fromisoformat(row["Date"])
            if trading_date.isoformat() != row["Date"]:
                raise ValueError("Date must be a cleaned YYYY-MM-DD date")
            high = Decimal(str(row["High"]))
            low = Decimal(str(row["Low"]))
            if not high.is_finite() or not low.is_finite():
                raise ValueError("High and Low must be finite numbers")
            if high <= 0 or low <= 0:
                raise ValueError("High and Low must be positive")
            if low > high:
                raise ValueError("Low must not exceed High")
        except (KeyError, TypeError, ValueError, InvalidOperation) as error:
            raise ValueError(f"Invalid analysis row {row_number}: {error}") from error
        prepared.append((trading_date, high, low, row))

    if not prepared:
        LOGGER.info("Clean window is empty; no highest or lowest stock price")
        return {"highest": None, "lowest": None}

    # max/min retain the first match, so date ordering resolves price ties.
    prepared.sort(key=lambda item: item[0])
    highest = max(prepared, key=lambda item: item[1])[3]
    lowest = min(prepared, key=lambda item: item[2])[3]
    LOGGER.info(
        "Analyzed %s clean rows: highest High=%s on %s; lowest Low=%s on %s",
        len(prepared), highest["High"], highest["Date"], lowest["Low"], lowest["Date"],
    )
    return {"highest": dict(highest), "lowest": dict(lowest)}
