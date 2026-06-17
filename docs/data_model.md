# Data Model

## Entity relationships

```
store_products (1) ----< store_sales_order_detail >---- (1) store_sales_order_header
        ProductID                              SalesOrderID
```

| Table | Primary key | Validation result |
| --- | --- | --- |
| `store_products` | `ProductID` | **VIOLATED in source**: 303 rows / 295 distinct IDs (8 conflicting duplicates) — resolved in `publish_product`, see `docs/assumptions.md` §3 |
| `store_sales_order_header` | `SalesOrderID` | Valid (31,465 / 31,465) |
| `store_sales_order_detail` | `SalesOrderDetailID` (unique on its own; `SalesOrderID + SalesOrderDetailID` also unique) | Valid (121,317 / 121,317) |

| Foreign key | Validation result |
| --- | --- |
| `sales_order_detail.SalesOrderID → sales_order_header.SalesOrderID` | Valid — 0 orphans |
| `sales_order_detail.ProductID → products.ProductID` | Valid — 0 orphans |

Every header order has at least one detail line, and `SalesPersonID` is NULL
exactly on the 27,659 rows where `OnlineOrderFlag = true` (online orders have
no salesperson — expected pattern, not a defect).

## Column types and justification

### store_products

| Column | Type | Justification |
| --- | --- | --- |
| ProductID | INT | Integer surrogate key (680–999 range) |
| ProductDesc | STRING | Free text |
| ProductNumber | STRING | Alphanumeric code (`FR-R92B-58`) |
| MakeFlag | BOOLEAN | Source values `True`/`False` |
| Color | STRING | Categorical |
| SafetyStockLevel | INT | Whole units |
| ReorderPoint | INT | Whole units |
| StandardCost | DECIMAL(19,4) | Money; source carries 4 decimals — exact arithmetic, no float drift |
| ListPrice | DECIMAL(19,4) | Money |
| Size | STRING | Mixed domain: `38`–`70` **and** `S/M/L/XL` — cannot be numeric |
| SizeUnitMeasureCode | STRING | Code (`CM`) |
| Weight | DECIMAL(9,2) | Source carries 2 decimals |
| WeightUnitMeasureCode | STRING | Code (`LB`/`G`) |
| ProductCategoryName | STRING | Categorical (NULL in 190/303 raw rows — enriched at publish) |
| ProductSubCategoryName | STRING | Categorical |

### store_sales_order_header

| Column | Type | Justification |
| --- | --- | --- |
| SalesOrderID | INT | Integer surrogate key |
| OrderDate | DATE | No time component in source. 5 rows arrive as `YYYY-MM`; parsed as the 1st of the month with precision recorded (below) |
| OrderDatePrecision | STRING (`day`/`month`) | **Technical column added by the pipeline** to make the partial-date handling auditable downstream |
| ShipDate | DATE | All values full ISO dates |
| OnlineOrderFlag | BOOLEAN | Source values `True`/`False` |
| AccountNumber | STRING | Pattern `99-9999-999999` — an identifier, not a number (leading zeros matter) |
| CustomerID | INT | Integer key |
| SalesPersonID | INT (nullable) | Integer key; NULL for online orders |
| Freight | DECIMAL(19,4) | Money |

### store_sales_order_detail

| Column | Type | Justification |
| --- | --- | --- |
| SalesOrderID | INT | FK to header |
| SalesOrderDetailID | INT | PK |
| OrderQty | INT | Whole units; **signed** — two `-1` return/correction lines exist and are preserved |
| ProductID | INT | FK to products |
| UnitPrice | DECIMAL(19,4) | Money (values like `.0000` parse cleanly) |
| UnitPriceDiscount | DECIMAL(19,4) | Kept in money precision because the case-study formula uses it as an amount (see `docs/assumptions.md` §4) |

## Publish layer

* **publish_product** (295 rows): store_products schema after deduplication,
  `Color` NULL→`N/A`, and category enrichment.
* **publish_orders** (121,317 rows — one per detail line): all detail columns,
  all header columns except `SalesOrderID` (with `Freight` →
  `TotalOrderFreight`), plus `LeadTimeInBusinessDays` (INT, nullable) and
  `TotalLineExtendedPrice` (DECIMAL(19,4)).
