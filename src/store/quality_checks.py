"""Data quality checks and profiling for the store layer.

Produces a human-readable markdown report (data/output/quality_report.md)
covering, per table:
    * row counts and per-column NULL counts (profiling)
    * primary-key uniqueness
    * cast failures (value present in raw but NULL after typing)
plus cross-table foreign-key integrity and dataset-specific anomalies
(duplicate ProductIDs, partial OrderDates, non-positive quantities,
zero unit prices, ship-before-order rows).

The checks REPORT; they do not mutate data. Resolution of the one real key
violation (duplicate ProductIDs) is a documented business rule applied in
the publish layer.
"""
from __future__ import annotations

from functools import reduce
from operator import or_

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import config
from src.store.schemas import FOREIGN_KEYS, PRIMARY_KEYS, STORE_SCHEMAS
from src.store.transform_store import normalize_str, typed_col


# ---------------------------------------------------------------------------
# Generic checks
# ---------------------------------------------------------------------------
def null_profile(df: DataFrame) -> dict[str, int]:
    agg = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    ).collect()[0]
    return {c: int(agg[c] or 0) for c in df.columns}


def check_primary_key(df: DataFrame, key_cols: list[str]) -> dict:
    total = df.count()
    distinct = df.select(*key_cols).distinct().count()
    any_null = reduce(or_, [F.col(c).isNull() for c in key_cols])
    null_keys = df.filter(any_null).count()
    return {
        "key": key_cols,
        "rows": total,
        "distinct_keys": distinct,
        "duplicate_rows": total - distinct,
        "null_keys": null_keys,
        "valid": total == distinct and null_keys == 0,
    }


def check_foreign_key(child: DataFrame, child_col: str,
                      parent: DataFrame, parent_col: str) -> dict:
    orphans = (
        child.filter(F.col(child_col).isNotNull())
        .join(parent.select(F.col(parent_col).alias("__pk")).distinct(),
              on=F.col(child_col) == F.col("__pk"), how="left_anti")
        .count()
    )
    return {"child_col": child_col, "parent_col": parent_col,
            "orphans": orphans, "valid": orphans == 0}


def count_cast_failures(name: str, raw_df: DataFrame) -> dict[str, int]:
    """Rows where the raw value is non-null but the typed value is NULL."""
    schema = STORE_SCHEMAS[name]
    exprs = []
    for col_name, target_type in schema.items():
        if target_type.upper() == "STRING":
            continue
        norm = normalize_str(col_name)
        if name == "sales_order_header" and col_name == "OrderDate":
            # partial dates are handled by design, not cast failures:
            # only count values that match NO supported pattern.
            typed = (
                F.when(norm.rlike(r"^\d{4}-\d{2}-\d{2}$"), norm.try_cast("DATE"))
                .when(norm.rlike(r"^\d{4}-\d{2}$"),
                      F.concat(norm, F.lit("-01")).try_cast("DATE"))
                .otherwise(F.lit(None).cast("date"))
            )
        else:
            typed = typed_col(col_name, target_type, source=norm)
        exprs.append(
            F.sum((norm.isNotNull() & typed.isNull()).cast("int")).alias(col_name)
        )
    if not exprs:
        return {}
    row = raw_df.select(*exprs).collect()[0]
    return {c: int(row[c] or 0) for c in row.asDict()}


# ---------------------------------------------------------------------------
# Dataset-specific anomaly checks
# ---------------------------------------------------------------------------
def anomaly_checks(store: dict[str, DataFrame]) -> dict:
    products = store["products"]
    header = store["sales_order_header"]
    detail = store["sales_order_detail"]

    dup_ids = [
        r["ProductID"]
        for r in products.groupBy("ProductID").count()
        .filter("count > 1").orderBy("ProductID").collect()
    ]
    partial_dates = [
        r["SalesOrderID"]
        for r in header.filter(F.col("OrderDatePrecision") == "month")
        .orderBy("SalesOrderID").collect()
    ]
    return {
        "duplicate_product_ids": dup_ids,
        "partial_order_date_sales_order_ids": partial_dates,
        "detail_rows_orderqty_le_0": detail.filter("OrderQty <= 0").count(),
        "detail_rows_unitprice_eq_0": detail.filter("UnitPrice = 0").count(),
        "header_rows_ship_before_order": header.filter(
            "ShipDate < OrderDate AND OrderDatePrecision = 'day'").count(),
    }


# ---------------------------------------------------------------------------
# Orchestration + report rendering
# ---------------------------------------------------------------------------
def run_quality_checks(raw: dict[str, DataFrame],
                       store: dict[str, DataFrame]) -> dict:
    report: dict = {"tables": {}, "foreign_keys": [], "anomalies": {}}

    for name in config.TABLES:
        report["tables"][name] = {
            "nulls": null_profile(store[name]),
            "primary_key": check_primary_key(store[name], PRIMARY_KEYS[name]),
            "cast_failures": count_cast_failures(name, raw[name]),
        }

    for child_t, child_c, parent_t, parent_c in FOREIGN_KEYS:
        fk = check_foreign_key(store[child_t], child_c, store[parent_t], parent_c)
        fk["relation"] = f"{child_t}.{child_c} -> {parent_t}.{parent_c}"
        report["foreign_keys"].append(fk)

    report["anomalies"] = anomaly_checks(store)
    _write_markdown(report)
    _print_summary(report)
    return report


def _write_markdown(report: dict) -> None:
    lines = ["# Data Quality Report", ""]
    for name, t in report["tables"].items():
        pk = t["primary_key"]
        lines += [
            f"## store_{name}",
            "",
            f"- Rows: **{pk['rows']:,}**",
            f"- Primary key `{', '.join(pk['key'])}`: "
            f"{'VALID' if pk['valid'] else 'VIOLATED'} "
            f"({pk['distinct_keys']:,} distinct keys, "
            f"{pk['duplicate_rows']} duplicate rows, {pk['null_keys']} null keys)",
            "",
            "| Column | NULLs | Cast failures |",
            "| --- | ---: | ---: |",
        ]
        for col, nulls in t["nulls"].items():
            cf = t["cast_failures"].get(col, "-")
            lines.append(f"| {col} | {nulls:,} | {cf} |")
        lines.append("")

    lines += ["## Foreign keys", ""]
    for fk in report["foreign_keys"]:
        lines.append(f"- `{fk['relation']}`: "
                     f"{'VALID' if fk['valid'] else 'VIOLATED'} "
                     f"({fk['orphans']} orphan rows)")
    lines += ["", "## Anomalies", ""]
    a = report["anomalies"]
    lines += [
        f"- Duplicate ProductIDs in products ({len(a['duplicate_product_ids'])}): "
        f"{a['duplicate_product_ids']} - resolved in publish_product "
        f"(deduplication rule in docs/assumptions.md).",
        f"- Header rows with month-only OrderDate "
        f"({len(a['partial_order_date_sales_order_ids'])}): "
        f"SalesOrderIDs {a['partial_order_date_sales_order_ids']} - "
        f"LeadTimeInBusinessDays is NULL for these rows.",
        f"- Detail rows with OrderQty <= 0: {a['detail_rows_orderqty_le_0']} "
        f"(kept; treated as returns/corrections - extended price goes negative).",
        f"- Detail rows with UnitPrice = 0: {a['detail_rows_unitprice_eq_0']} "
        f"(kept; promotional/giveaway lines).",
        f"- Header rows with ShipDate < OrderDate: "
        f"{a['header_rows_ship_before_order']}.",
        "",
    ]
    config.QUALITY_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    config.QUALITY_REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def _print_summary(report: dict) -> None:
    print(f"[quality] report written -> {config.QUALITY_REPORT_PATH}")
    for name, t in report["tables"].items():
        pk = t["primary_key"]
        status = "OK" if pk["valid"] else f"VIOLATED ({pk['duplicate_rows']} dups)"
        print(f"[quality] store_{name}: PK {pk['key']} {status}")
    for fk in report["foreign_keys"]:
        status = "OK" if fk["valid"] else f"{fk['orphans']} orphans"
        print(f"[quality] FK {fk['relation']}: {status}")
