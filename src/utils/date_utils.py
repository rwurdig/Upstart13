"""Business-day calculation utilities.

Convention (documented in docs/assumptions.md):
    LeadTimeInBusinessDays counts the business days (Mon-Fri) **after** the
    start date up to and **including** the end date, i.e. start-exclusive /
    end-inclusive. Examples:
        Mon -> Fri (same week)  = 4
        Fri -> Mon              = 1
        any day -> same day     = 0
    Saturdays and Sundays are never counted. No holiday calendar is applied
    (the case study explicitly scopes the exclusion to weekends only).

Two implementations are provided and unit-tested for parity:
    * ``business_days_between`` - pure Python, used in tests and validation.
    * ``business_days_col``     - a Spark Column expression used by the
      pipeline (no UDF, so Catalyst can optimise it and there is no Python
      serialisation overhead per row).
"""
from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import Column
from pyspark.sql import functions as F


def business_days_between(start: date | None, end: date | None) -> int | None:
    """Business days from ``start`` (exclusive) to ``end`` (inclusive).

    Returns ``None`` when either date is missing, mirroring NULL semantics in
    the Spark implementation. A negative range returns the negated count of
    the reversed range (not expected in this dataset, but deterministic).
    """
    if start is None or end is None:
        return None
    if end < start:
        reversed_count = business_days_between(end, start)
        return -reversed_count if reversed_count is not None else None

    days = 0
    current = start
    while current < end:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Mon=0 .. Fri=4
            days += 1
    return days


def business_days_col(start_col: str, end_col: str) -> Column:
    """Spark Column computing business days between two DATE columns.

    Pure Spark SQL (sequence + filter + size): deterministic, ANSI-safe and
    UDF-free. NULL when either side is NULL or when end < start (flagged
    separately by the quality checks; never observed in this dataset).
    """
    # dayofweek(): 1 = Sunday, 7 = Saturday (works on every Spark 3.x/4.x)
    counting_expr = F.expr(
        f"size(filter(sequence(date_add(`{start_col}`, 1), `{end_col}`), "
        f"d -> dayofweek(d) NOT IN (1, 7)))"
    )
    return (
        F.when(F.col(start_col).isNull() | F.col(end_col).isNull(), F.lit(None).cast("int"))
        .when(F.col(end_col) < F.col(start_col), F.lit(None).cast("int"))
        .when(F.col(end_col) == F.col(start_col), F.lit(0))
        .otherwise(counting_expr)
        .cast("int")
    )
