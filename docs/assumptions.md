# Assumptions & Trade-offs

Numbered so the README and code comments can reference them.

## 1. Business-day convention

`LeadTimeInBusinessDays` counts business days (Mon–Fri) **after** `OrderDate`
up to and **including** `ShipDate` — start-exclusive / end-inclusive.

* order Monday, ship Friday → 4
* order Friday, ship Monday → 1
* ship 7 calendar days after order (the dominant pattern: 30,984 of 31,460
  full-date orders) → 5

Only Saturdays and Sundays are excluded, exactly as the case study specifies —
no holiday calendar is applied. The Spark implementation is UDF-free
(`sequence` + `filter` + `size`) and is unit-tested for parity against a pure
Python reference (`tests/test_date_utils.py`).

## 2. Partial OrderDate values (`YYYY-MM`)

Five header rows (SalesOrderIDs 43828–43832) carry `OrderDate` truncated to
year-month. Handling:

* The date is parsed as the **1st of the month** so the rows keep a typed
  DATE and remain usable for year-level analytics (Q1 revenue by year).
* A technical column `OrderDatePrecision` (`day`/`month`) records the fact.
* `LeadTimeInBusinessDays` is **NULL** for these rows: a lead time computed
  from an imputed day would be a fabricated metric (it would read ~24
  business days against a true norm of 5).

Trade-off considered: setting `OrderDate` itself to NULL would have been
simpler but would silently drop ≈ $17.7K of 2021 revenue from Q1. The chosen
approach keeps revenue complete and lead time honest. Verified by
reconciliation: 2021 "Red" revenue = 6,005,300.94 (full-date orders)
\+ 14,313.08 (partial-date orders) = **6,019,614.02**.

## 3. Duplicate ProductIDs

`products.csv` contains 303 rows but 295 distinct `ProductID`s. Eight jersey
products (713–716, 881–884) appear twice with conflicting attributes:

* one row with `ProductCategoryName = NULL`, `ProductSubCategoryName = 'Jerseys'`
* one row with `ProductCategoryName = 'Clothing'`, `ProductSubCategoryName = 'Shirt'`

Rule applied in `publish_product` (the store layer stays row-faithful and the
violation is flagged in the quality report): keep the **most complete** row
per `ProductID` — rows with a non-NULL category (and subcategory) win; ties
break deterministically by `ProductSubCategoryName`, then `ProductNumber`.
For all 8 products this keeps the `Clothing`/`Shirt` record.

Why it matters: without deduplication, every join on `ProductID`
(including analysis Q1/Q2) would double-count revenue for these products.

## 4. UnitPriceDiscount semantics

The source values (0, 0.02, 0.05 … 0.40) are clearly **discount rates**; the
canonical line total would be `OrderQty * UnitPrice * (1 - UnitPriceDiscount)`.
The case study, however, explicitly defines
`TotalLineExtendedPrice = OrderQty * (UnitPrice - UnitPriceDiscount)`,
treating the value as an absolute amount. **The specification is implemented
as written** (specs win; the deviation is documented here and is immaterial —
at most $0.40 per unit). This is flagged as a question to raise with the
client rather than silently "corrected".

## 5. NULL normalisation

In the store layer all values are trimmed; empty strings and the literal
`NULL` (any casing) become real NULLs before casting. Casts use Spark's
`try_cast` (Spark 4 runs ANSI mode by default, where plain CAST throws), and
every non-string column is audited for cast failures — observed: **zero**.

## 6. Anomalous order lines are preserved

* 2 detail lines with `OrderQty = -1` → treated as returns/corrections; their
  `TotalLineExtendedPrice` is legitimately negative.
* 94 detail lines with `UnitPrice = 0` → promotional/giveaway lines.

Both are counted in the quality report; neither is dropped (revenue questions
should reflect the books as delivered).

## 7. Category mapping is exact-match (+ one substring rule)

Subcategory matching uses exact, case-sensitive equality, and the
`'Frames'` rule uses a case-sensitive substring match — both exactly as
specified. Consequences embraced: `Bib-Shorts` is **not** `Shorts` and stays
NULL; `Mountain/Road/Touring Frames` all map to `Components`. Subcategories
outside the mapping (Brakes, Chains, Caps, …) keep a NULL category by design;
they are displayed as `(Uncategorized)` in analysis output only.

## 8. Join strategy

`publish_orders` uses a **left** join from detail to header (defensive: a
detail line must never disappear because its header is missing). FK
validation shows 0 orphans, so it is equivalent to an inner join here.

## 9. `N/A` participates in Q1 as a color

After the required NULL→`N/A` replacement, `N/A` competes as a color value in
"which color generated the highest revenue". It never ranks first in any
year, so no caveat is needed in the answers.

## 10. Money precision

All monetary columns are `DECIMAL(19,4)` — matching the 4 decimal places in
the source and avoiding floating-point drift in revenue sums.
