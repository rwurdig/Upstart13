"""Store layer: typed, analysis-ready versions of the raw tables.

Principles:
    * Row-faithful: the store layer never drops or fabricates rows. Key
      violations and anomalies are *reported* (quality_checks) and resolved
      in the publish layer where a business rule can be stated explicitly.
    * Defensive typing: every cast uses Spark's ``try_cast`` so malformed
      values become NULL instead of failing the job (Spark 4 runs in ANSI
      mode by default, where a plain CAST would throw). Cast failures are
      audited by quality_checks.count_cast_failures - nothing is silently
      lost without being counted.
    * NULL normalisation: values are trimmed; empty strings and the literal
      'NULL' (any case) are converted to real NULLs before casting.

Special case - partial OrderDate values:
    5 header rows carry OrderDate as 'YYYY-MM' (day truncated). We keep the
    rows and the year/month information by parsing them as the 1st of the
    month, and we record the precision in a technical column
    ``OrderDatePrecision`` ('day' | 'month'). Downstream,
    LeadTimeInBusinessDays is set to NULL for month-precision rows: a lead
    time computed from an imputed day would be fabricated, while yearly
    revenue (analysis Q1) remains correct. See docs/assumptions.md.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src import config
from src.store.schemas import STORE_SCHEMAS

DATE_FULL_RE = r"^\d{4}-\d{2}-\d{2}$"
DATE_MONTH_RE = r"^\d{4}-\d{2}$"

ORDER_DATE_PRECISION_COL = "OrderDatePrecision"


def normalize_str(col_name: str) -> Column:
    """Trim and convert ''/'NULL' (any case) to real NULL."""
    trimmed = F.trim(F.col(col_name))
    return F.when(
        F.col(col_name).isNull() | (trimmed == "") | (F.upper(trimmed) == "NULL"),
        F.lit(None).cast("string"),
    ).otherwise(trimmed)


def typed_col(col_name: str, target_type: str, source: Column | None = None) -> Column:
    """ANSI-safe cast of a normalised string column to its target type."""
    src = source if source is not None else normalize_str(col_name)
    if target_type.upper() == "STRING":
        return src
    return src.try_cast(target_type)


def order_date_columns() -> tuple[Column, Column]:
    """(OrderDate as DATE, OrderDatePrecision) handling partial 'YYYY-MM'."""
    norm = normalize_str("OrderDate")
    precision = (
        F.when(norm.isNull(), F.lit(None).cast("string"))
        .when(norm.rlike(DATE_FULL_RE), F.lit("day"))
        .when(norm.rlike(DATE_MONTH_RE), F.lit("month"))
        .otherwise(F.lit(None).cast("string"))
    )
    order_date = (
        F.when(norm.rlike(DATE_FULL_RE), norm.try_cast("DATE"))
        .when(norm.rlike(DATE_MONTH_RE), F.concat(norm, F.lit("-01")).try_cast("DATE"))
        .otherwise(F.lit(None).cast("date"))
    )
    return order_date, precision


def transform_table(name: str, raw_df: DataFrame) -> DataFrame:
    """Apply normalisation + typing for one table."""
    schema = STORE_SCHEMAS[name]
    select_exprs: list[Column] = []

    for col_name, target_type in schema.items():
        if name == "sales_order_header" and col_name == "OrderDate":
            order_date, precision = order_date_columns()
            select_exprs.append(order_date.alias("OrderDate"))
            select_exprs.append(precision.alias(ORDER_DATE_PRECISION_COL))
        else:
            select_exprs.append(typed_col(col_name, target_type).alias(col_name))

    return raw_df.select(*select_exprs)


def build_store(raw_frames: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Type every raw table and persist it as a store_* parquet table."""
    spark = next(iter(raw_frames.values())).sparkSession
    store_frames: dict[str, DataFrame] = {}

    for name, raw_df in raw_frames.items():
        typed = transform_table(name, raw_df)
        typed.write.mode("overwrite").parquet(config.store_path(name))
        store_frames[name] = spark.read.parquet(config.store_path(name))
        print(f"[store] store_{name}: {store_frames[name].count():,} rows "
              f"-> {config.store_path(name)}")

    return store_frames
