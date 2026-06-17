"""publish_product: business-ready product master.

Transformations (in order):
    1. Deduplicate ProductID. The source contains 8 products listed twice
       with conflicting attributes (one row with NULL category/'Jerseys'
       subcategory, one row with 'Clothing'/'Shirt'). Rule: keep the most
       complete row per ProductID - rows with a non-NULL ProductCategoryName
       win; ties broken deterministically (see docs/assumptions.md). Without
       this step every downstream join on ProductID would double-count.
    2. Replace NULL Color with the literal 'N/A' (case-study requirement).
    3. Enrich ProductCategoryName when NULL (case-study mapping):
         - Gloves/Shorts/Socks/Tights/Vests                  -> Clothing
         - Locks/Lights/Headsets/Helmets/Pedals/Pumps        -> Accessories
         - contains 'Frames', or Wheels/Saddles              -> Components
       Non-NULL categories are never overwritten; subcategories outside the
       mapping remain NULL by design.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src import config

CLOTHING_SUBCATS = ["Gloves", "Shorts", "Socks", "Tights", "Vests"]
ACCESSORY_SUBCATS = ["Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"]
COMPONENT_EXACT_SUBCATS = ["Wheels", "Saddles"]
COMPONENT_CONTAINS_TOKEN = "Frames"


def deduplicate_products(products: DataFrame) -> DataFrame:
    """Keep one row per ProductID, preferring the most complete record."""
    completeness_rank = (
        F.col("ProductCategoryName").isNull().cast("int")  # non-null category first
        + F.col("ProductSubCategoryName").isNull().cast("int")
    )
    window = Window.partitionBy("ProductID").orderBy(
        completeness_rank.asc(),
        F.col("ProductSubCategoryName").asc_nulls_last(),  # deterministic tie-break
        F.col("ProductNumber").asc_nulls_last(),
    )
    return (
        products.withColumn("__rn", F.row_number().over(window))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )


def enrich_category(products: DataFrame) -> DataFrame:
    sub = F.col("ProductSubCategoryName")
    cat = F.col("ProductCategoryName")
    enriched = (
        F.when(cat.isNotNull(), cat)
        .when(sub.isin(CLOTHING_SUBCATS), F.lit("Clothing"))
        .when(sub.isin(ACCESSORY_SUBCATS), F.lit("Accessories"))
        .when(
            sub.contains(COMPONENT_CONTAINS_TOKEN) | sub.isin(COMPONENT_EXACT_SUBCATS),
            F.lit("Components"),
        )
        .otherwise(F.lit(None).cast("string"))
    )
    return products.withColumn("ProductCategoryName", enriched)


def transform_product(store_products: DataFrame) -> DataFrame:
    """Full publish_product transformation (pure; no I/O - unit-testable)."""
    deduped = deduplicate_products(store_products)
    colored = deduped.withColumn("Color", F.coalesce(F.col("Color"), F.lit("N/A")))
    return enrich_category(colored)


def build_publish_product(store_products: DataFrame) -> DataFrame:
    spark = store_products.sparkSession
    result = transform_product(store_products)
    result.write.mode("overwrite").parquet(config.publish_path("product"))
    out = spark.read.parquet(config.publish_path("product"))
    print(f"[publish] publish_product: {out.count():,} rows "
          f"-> {config.publish_path('product')}")
    return out
