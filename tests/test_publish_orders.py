"""Tests for the publish_orders transformation."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from src.publish.publish_orders import transform_orders

DETAIL_SCHEMA = (
    "SalesOrderID int, SalesOrderDetailID int, OrderQty int, ProductID int, "
    "UnitPrice decimal(19,4), UnitPriceDiscount decimal(19,4)"
)
HEADER_SCHEMA = (
    "SalesOrderID int, OrderDate date, OrderDatePrecision string, "
    "ShipDate date, OnlineOrderFlag boolean, AccountNumber string, "
    "CustomerID int, SalesPersonID int, Freight decimal(19,4)"
)


@pytest.fixture()
def frames(spark):
    detail = spark.createDataFrame(
        [
            (1, 10, 2, 100, Decimal("100.0000"), Decimal("0.0000")),
            (1, 11, 3, 101, Decimal("50.0000"), Decimal("0.1000")),
            (2, 12, -1, 100, Decimal("20.0000"), Decimal("0.0000")),
            (3, 13, 1, 102, Decimal("10.0000"), Decimal("0.0000")),
        ],
        DETAIL_SCHEMA,
    )
    header = spark.createDataFrame(
        [
            # Mon 2026-06-01 -> Mon 2026-06-08: 5 business days
            (1, date(2026, 6, 1), "day", date(2026, 6, 8), True,
             "10-1", 7, None, Decimal("9.9900")),
            # Fri 2026-06-05 -> Mon 2026-06-08: 1 business day
            (2, date(2026, 6, 5), "day", date(2026, 6, 8), False,
             "10-2", 8, 281, Decimal("5.0000")),
            # month-precision order date -> lead time must be NULL
            (3, date(2026, 6, 1), "month", date(2026, 6, 8), True,
             "10-3", 9, None, Decimal("1.0000")),
        ],
        HEADER_SCHEMA,
    )
    return detail, header


def test_column_contract(frames):
    detail, header = frames
    out = transform_orders(detail, header)

    expected = (
        set(detail.columns)
        | (set(header.columns) - {"SalesOrderID", "Freight"})
        | {"TotalOrderFreight", "LeadTimeInBusinessDays", "TotalLineExtendedPrice"}
    )
    assert set(out.columns) == expected
    assert out.columns.count("SalesOrderID") == 1   # join key appears once
    assert "Freight" not in out.columns             # renamed away


def test_row_grain_preserved(frames):
    detail, header = frames
    assert transform_orders(detail, header).count() == detail.count()


def test_derived_columns(frames):
    detail, header = frames
    rows = {r["SalesOrderDetailID"]: r
            for r in transform_orders(detail, header).collect()}

    # lead time: Mon->next Mon = 5; Fri->Mon = 1; month precision -> NULL
    assert rows[10]["LeadTimeInBusinessDays"] == 5
    assert rows[12]["LeadTimeInBusinessDays"] == 1
    assert rows[13]["LeadTimeInBusinessDays"] is None

    # extended price = OrderQty * (UnitPrice - UnitPriceDiscount)
    assert rows[10]["TotalLineExtendedPrice"] == Decimal("200.0000")
    assert rows[11]["TotalLineExtendedPrice"] == Decimal("149.7000")
    assert rows[12]["TotalLineExtendedPrice"] == Decimal("-20.0000")  # return line

    # order-level attributes carried to every line
    assert rows[10]["TotalOrderFreight"] == Decimal("9.9900")
    assert rows[12]["OnlineOrderFlag"] is False
