"""Tests for the business-day calculation - the riskiest logic in the repo.

Convention under test: start-exclusive / end-inclusive, weekends never
counted (2026-06-01 is a Monday; used as the anchor week below).
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from src.utils.date_utils import business_days_between, business_days_col

MON = date(2026, 6, 1)
TUE = date(2026, 6, 2)
FRI = date(2026, 6, 5)
SAT = date(2026, 6, 6)
SUN = date(2026, 6, 7)
NEXT_MON = date(2026, 6, 8)
NEXT_FRI = date(2026, 6, 12)


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        (MON, MON, 0),              # same day
        (MON, TUE, 1),              # single weekday step
        (MON, FRI, 4),              # Mon -> Fri, same week
        (FRI, NEXT_MON, 1),         # weekend fully skipped
        (FRI, NEXT_FRI, 5),         # full calendar week = 5 business days
        (SAT, SUN, 0),              # weekend-only span
        (SAT, NEXT_MON, 1),         # start on weekend
        (FRI, SAT, 0),              # end on Saturday
        (FRI, SUN, 0),              # end on Sunday
        (MON, NEXT_FRI, 9),         # across two weeks
        (None, FRI, None),          # missing start
        (MON, None, None),          # missing end
        (FRI, MON, -4),             # reversed range -> negated
    ],
)
def test_business_days_between(start, end, expected):
    assert business_days_between(start, end) == expected


def test_seven_calendar_days_is_five_business_days():
    """The dominant pattern in the dataset: ship 7 calendar days after order."""
    for offset in range(7):  # every possible starting weekday
        start = MON + timedelta(days=offset)
        assert business_days_between(start, start + timedelta(days=7)) == 5


def test_spark_parity_with_python(spark):
    """The Spark column expression must match the pure-Python reference."""
    pairs = [
        (MON + timedelta(days=i), MON + timedelta(days=i + j))
        for i in range(7)
        for j in range(0, 15)
    ] + [(None, FRI), (MON, None)]

    df = spark.createDataFrame(pairs, "OrderDate date, ShipDate date")
    rows = (
        df.withColumn("bd", business_days_col("OrderDate", "ShipDate"))
        .collect()
    )
    for row in rows:
        assert row["bd"] == business_days_between(row["OrderDate"], row["ShipDate"]), (
            f"Mismatch for {row['OrderDate']} -> {row['ShipDate']}"
        )
