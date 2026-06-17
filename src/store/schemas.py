"""Target schemas and key definitions for the store layer.

Type decisions (full justification in docs/data_model.md):
    * IDs and quantities      -> INT (values fit comfortably; OrderQty can be
                                 negative - returns/corrections exist in data)
    * Monetary amounts        -> DECIMAL(19,4) (source carries 4 decimals;
                                 exact arithmetic, no float drift)
    * UnitPriceDiscount       -> DECIMAL(19,4) (kept in money precision; the
                                 case-study formula treats it as an amount)
    * Dates                   -> DATE (no time component in the source)
    * True/False flags        -> BOOLEAN
    * Size                    -> STRING (mixed domain: 38..70 and S/M/L/XL)
    * Everything else         -> STRING
"""
from __future__ import annotations

# column -> Spark SQL type used with try_cast()
STORE_SCHEMAS: dict[str, dict[str, str]] = {
    "products": {
        "ProductID": "INT",
        "ProductDesc": "STRING",
        "ProductNumber": "STRING",
        "MakeFlag": "BOOLEAN",
        "Color": "STRING",
        "SafetyStockLevel": "INT",
        "ReorderPoint": "INT",
        "StandardCost": "DECIMAL(19,4)",
        "ListPrice": "DECIMAL(19,4)",
        "Size": "STRING",
        "SizeUnitMeasureCode": "STRING",
        "Weight": "DECIMAL(9,2)",
        "WeightUnitMeasureCode": "STRING",
        "ProductCategoryName": "STRING",
        "ProductSubCategoryName": "STRING",
    },
    "sales_order_header": {
        "SalesOrderID": "INT",
        # OrderDate is handled specially in transform_store (partial dates);
        # the declared target type is still DATE.
        "OrderDate": "DATE",
        "ShipDate": "DATE",
        "OnlineOrderFlag": "BOOLEAN",
        "AccountNumber": "STRING",
        "CustomerID": "INT",
        "SalesPersonID": "INT",
        "Freight": "DECIMAL(19,4)",
    },
    "sales_order_detail": {
        "SalesOrderID": "INT",
        "SalesOrderDetailID": "INT",
        "OrderQty": "INT",
        "ProductID": "INT",
        "UnitPrice": "DECIMAL(19,4)",
        "UnitPriceDiscount": "DECIMAL(19,4)",
    },
}

# Primary keys (validated, not enforced, by quality_checks - the store layer
# stays row-faithful to raw; key violations are resolved in publish).
PRIMARY_KEYS: dict[str, list[str]] = {
    "products": ["ProductID"],
    "sales_order_header": ["SalesOrderID"],
    "sales_order_detail": ["SalesOrderDetailID"],
}

# Foreign keys: (child_table, child_col, parent_table, parent_col)
FOREIGN_KEYS: list[tuple[str, str, str, str]] = [
    ("sales_order_detail", "SalesOrderID", "sales_order_header", "SalesOrderID"),
    ("sales_order_detail", "ProductID", "products", "ProductID"),
]
