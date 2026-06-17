"""publish_orders: order-line fact table.

Built by joining store_sales_order_detail (left) with
store_sales_order_header on SalesOrderID.

Column contract (case-study requirement):
    * all columns from SalesOrderDetail (including SalesOrderID, the grain
      key carrier),
    * all columns from SalesOrderHeader except SalesOrderID, with Freight
      renamed to TotalOrderFreight (it is an order-level amount repeated on
      every line - the rename prevents accidental summing per line),
    * LeadTimeInBusinessDays - business days between OrderDate and ShipDate
      excluding Saturdays/Sundays (start-exclusive, end-inclusive; see
      src/utils/date_utils.py). NULL when OrderDate is month-precision:
      computing it from an imputed day would fabricate a metric.
    * TotalLineExtendedPrice = OrderQty * (UnitPrice - UnitPriceDiscount),
      exactly as specified by the case study (see docs/assumptions.md for a
      note on the discount semantics).

A left join is used defensively; FK validation showed zero orphan detail
rows, so it is equivalent to an inner join on this dataset.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import config
from src.utils.date_utils import business_days_col

LEAD_TIME_COL = "LeadTimeInBusinessDays"
EXTENDED_PRICE_COL = "TotalLineExtendedPrice"


def transform_orders(detail: DataFrame, header: DataFrame) -> DataFrame:
    """Full publish_orders transformation (pure; no I/O - unit-testable)."""
    header_renamed = header.withColumnRenamed("Freight", "TotalOrderFreight")
    header_cols = [c for c in header_renamed.columns if c != "SalesOrderID"]

    joined = detail.alias("d").join(
        header_renamed.alias("h"), on="SalesOrderID", how="left"
    )

    projected = joined.select(
        [F.col(f"d.{c}") for c in detail.columns]
        + [F.col(f"h.{c}") for c in header_cols]
    )

    with_lead_time = projected.withColumn(
        LEAD_TIME_COL,
        F.when(F.col("OrderDatePrecision") == "month", F.lit(None).cast("int"))
        .otherwise(business_days_col("OrderDate", "ShipDate")),
    )

    return with_lead_time.withColumn(
        EXTENDED_PRICE_COL,
        (
            F.col("OrderQty")
            * (F.col("UnitPrice") - F.col("UnitPriceDiscount"))
        ).cast("decimal(19,4)"),
    )


def build_publish_orders(detail: DataFrame, header: DataFrame) -> DataFrame:
    spark = detail.sparkSession
    result = transform_orders(detail, header)
    result.write.mode("overwrite").parquet(config.publish_path("orders"))
    out = spark.read.parquet(config.publish_path("orders"))
    print(f"[publish] publish_orders: {out.count():,} rows "
          f"-> {config.publish_path('orders')}")
    return out
