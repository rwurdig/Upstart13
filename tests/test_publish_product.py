"""Tests for the publish_product transformation."""
from __future__ import annotations

from src.publish.publish_product import transform_product

SCHEMA = (
    "ProductID int, ProductDesc string, ProductNumber string, Color string, "
    "ProductCategoryName string, ProductSubCategoryName string"
)


def _rows_by_id(df):
    return {r["ProductID"]: r for r in df.collect()}


def test_category_enrichment_rules(spark):
    data = [
        (1, "Gloves item", "P-1", "Red", None, "Gloves"),          # -> Clothing
        (2, "Helmet item", "P-2", "Blue", None, "Helmets"),        # -> Accessories
        (3, "Mtn frame", "P-3", "Black", None, "Mountain Frames"), # contains Frames
        (4, "Wheel item", "P-4", "Silver", None, "Wheels"),        # exact Components
        (5, "Saddle item", "P-5", None, None, "Saddles"),          # exact Components
        (6, "Cap item", "P-6", "Multi", None, "Caps"),             # unmapped -> NULL
        (7, "Bike item", "P-7", "Yellow", "Bikes", "Road Bikes"),  # untouched
        (8, "Bib item", "P-8", None, None, "Bib-Shorts"),          # NOT 'Shorts'
    ]
    out = _rows_by_id(transform_product(spark.createDataFrame(data, SCHEMA)))

    assert out[1]["ProductCategoryName"] == "Clothing"
    assert out[2]["ProductCategoryName"] == "Accessories"
    assert out[3]["ProductCategoryName"] == "Components"
    assert out[4]["ProductCategoryName"] == "Components"
    assert out[5]["ProductCategoryName"] == "Components"
    assert out[6]["ProductCategoryName"] is None          # outside the mapping
    assert out[7]["ProductCategoryName"] == "Bikes"       # never overwritten
    assert out[8]["ProductCategoryName"] is None          # exact match only


def test_color_null_replaced_with_na(spark):
    data = [
        (1, "A", "P-1", None, "Bikes", "Road Bikes"),
        (2, "B", "P-2", "Red", "Bikes", "Road Bikes"),
    ]
    out = _rows_by_id(transform_product(spark.createDataFrame(data, SCHEMA)))
    assert out[1]["Color"] == "N/A"
    assert out[2]["Color"] == "Red"


def test_deduplication_prefers_complete_row(spark):
    data = [
        # ProductID 713 twice: incomplete (NULL category) vs complete row
        (713, "Long-Sleeve Jersey S", "LJ-0192-S", "Multi", None, "Jerseys"),
        (713, "Long-Sleeve Jersey S", "LJ-0192-S", "Multi", "Clothing", "Shirt"),
        (999, "Unique item", "P-999", "Red", "Bikes", "Road Bikes"),
    ]
    result = transform_product(spark.createDataFrame(data, SCHEMA))
    out = _rows_by_id(result)

    assert result.count() == 2                                   # one row per ID
    assert out[713]["ProductCategoryName"] == "Clothing"         # complete row won
    assert out[713]["ProductSubCategoryName"] == "Shirt"
    assert out[999]["ProductDesc"] == "Unique item"


def test_dedup_of_two_incomplete_rows_is_deterministic(spark):
    data = [
        (5, "x", "P-5b", "Red", None, "ZSub"),
        (5, "x", "P-5a", "Red", None, "ASub"),
    ]
    result = transform_product(spark.createDataFrame(data, SCHEMA))
    assert result.count() == 1
    assert result.collect()[0]["ProductSubCategoryName"] == "ASub"  # alphabetical
