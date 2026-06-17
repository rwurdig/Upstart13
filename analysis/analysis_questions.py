"""Analysis questions, answered from the publish layer only.

Q1. Which color generated the highest revenue each year?
    Revenue = SUM(TotalLineExtendedPrice); year = YEAR(OrderDate); Color
    comes from publish_product joined on ProductID. Ties (none observed)
    would all be returned with rank 1.

Q2. What is the average LeadTimeInBusinessDays by ProductCategoryName?
    Month-precision orders (NULL lead time) are excluded by AVG semantics.
    Products whose subcategory is outside the case-study mapping keep a
    NULL category and are shown as '(Uncategorized)' for readability.

Results are printed and persisted as single-file CSVs under
data/output/analysis/.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src import config


def top_color_by_year(orders: DataFrame, product: DataFrame) -> DataFrame:
    revenue = (
        orders.join(product.select("ProductID", "Color"), on="ProductID", how="inner")
        .withColumn("OrderYear", F.year("OrderDate"))
        .groupBy("OrderYear", "Color")
        .agg(F.sum("TotalLineExtendedPrice").alias("Revenue"))
    )
    window = Window.partitionBy("OrderYear").orderBy(F.col("Revenue").desc())
    return (
        revenue.withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank")
        .orderBy("OrderYear")
    )


def avg_lead_time_by_category(orders: DataFrame, product: DataFrame) -> DataFrame:
    return (
        orders.join(
            product.select("ProductID", "ProductCategoryName"),
            on="ProductID", how="inner",
        )
        .groupBy("ProductCategoryName")
        .agg(
            F.round(F.avg("LeadTimeInBusinessDays"), 2).alias("AvgLeadTimeInBusinessDays"),
            F.count("LeadTimeInBusinessDays").alias("OrderLinesWithLeadTime"),
        )
        .withColumn(
            "ProductCategoryName",
            F.coalesce(F.col("ProductCategoryName"), F.lit("(Uncategorized)")),
        )
        .orderBy(F.col("AvgLeadTimeInBusinessDays").desc(), "ProductCategoryName")
    )


def _save_csv(df: DataFrame, name: str) -> None:
    config.ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    df.toPandas().to_csv(config.ANALYSIS_DIR / f"{name}.csv", index=False)


def run_analysis(orders: DataFrame, product: DataFrame) -> dict[str, DataFrame]:
    q1 = top_color_by_year(orders, product)
    q2 = avg_lead_time_by_category(orders, product)

    print("\n=== Q1: Top revenue color per year ===")
    q1.show(50, truncate=False)
    print("=== Q2: Average lead time (business days) by product category ===")
    q2.show(50, truncate=False)

    _save_csv(q1, "q1_top_color_by_year")
    _save_csv(q2, "q2_avg_lead_time_by_category")
    print(f"[analysis] results saved -> {config.ANALYSIS_DIR}")
    return {"q1": q1, "q2": q2}
