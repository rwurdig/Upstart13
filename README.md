# AdventureWorks Sales Pipeline (raw → store → publish)

A reproducible PySpark pipeline over three AdventureWorks-style CSV extracts
(products, sales order headers, sales order detail lines). It follows a
strict medallion-style layering — `raw_` → `store_` → `publish_` — runs
end-to-end with a single command, validates its own data quality, and answers
the two analysis questions below.

## Architecture

```text
CSV files              raw layer                store layer                publish layer
data/input/*.csv  ──►  raw_products        ──►  store_products        ──►  publish_product
                       raw_sales_order_*   ──►  store_sales_order_*   ──►  publish_orders ──► analysis (Q1, Q2)
                       (as-delivered,           (typed, NULL-normalised,   (business rules,
                         all strings)            keys validated,             derived columns)
                                                 quality report)
```

* **raw** — files loaded exactly as delivered, every column a string. A
  faithful, replayable copy; no cleaning.
* **store** — trimmed, `''`/`'NULL'` normalised to real NULLs, every column
  cast to a documented type with `try_cast` (ANSI-safe), cast failures
  audited (zero observed). Row-faithful: nothing is dropped here.
* **publish** — business-ready tables: deduplicated/enriched product master
  and an order-line fact table with the two derived metrics.
* **quality** — PK uniqueness, FK integrity, null profile, cast-failure audit
  and anomaly checks, written to `data/output/quality_report.md`.

All tables are persisted as Parquet under `data/output/{raw,store,publish}/`
with `mode=overwrite`, so the whole pipeline is idempotent.

## How to run

```bash
pip install -r requirements.txt     # PySpark 4.x requires Java 17 or 21
python -m src.main                  # full pipeline + analysis (~1 min locally)
python -m pytest tests/ -q          # 22 unit tests
```

`src/config.py` is the only place paths and Spark settings live.

## Analysis answers

### Q1 — Which color generated the highest revenue each year?

Revenue = `SUM(TotalLineExtendedPrice)`, year from `OrderDate`,
`publish_orders ⋈ publish_product` on `ProductID`.

| Year | Top color | Revenue ($) | Runner-up (context) |
| --- | --- | ---: | --- |
| 2021* | **Red** | 6,019,614.02 | Black — 3,727,280.29 |
| 2022 | **Black** | 14,005,242.98 | Red — 11,565,491.16 |
| 2023 | **Black** | 15,047,694.37 | Yellow — 10,638,314.92 |
| 2024* | **Yellow** | 6,368,158.48 | Black — 5,579,326.79 |

\* 2021 and 2024 are partial years (data spans 2021-05-31 → 2024-06-30).
Red dominates the early road-bike-heavy period, Black takes over as the
catalogue broadens, and Yellow's 2023 momentum carries it to the top of the
2024 half-year. `N/A` (colorless products) competes as a value but never
ranks first. The 2021 figure includes ≈$14.3K from five orders whose
`OrderDate` arrived as `YYYY-MM` (see findings below) — reconciled exactly.

### Q2 — Average `LeadTimeInBusinessDays` by `ProductCategoryName`

| ProductCategoryName | Avg lead time (business days) | Order lines |
| --- | ---: | ---: |
| (Uncategorized) | 5.01 | 37,653 |
| Accessories | 5.01 | 13,021 |
| Clothing | 5.01 | 23,880 |
| Bikes | 5.00 | 12,456 |
| Components | 5.00 | 34,302 |

Lead time is essentially flat at **5 business days** for every category —
consistent with the warehouse pattern of shipping 7 calendar days after the
order (30,984 of 31,460 orders), which is exactly 5 business days under the
documented convention. `(Uncategorized)` covers products whose subcategory
falls outside the case-study mapping (Brakes, Chains, Caps, …); the five
month-precision orders have NULL lead time and are excluded by `AVG`.

## Data quality findings

The dataset contains three deliberate-looking traps, all detected, handled
and reported in `data/output/quality_report.md`:

1. **Duplicate ProductIDs** — 303 product rows, 295 distinct IDs. Eight
   jerseys (713–716, 881–884) appear twice with conflicting category data.
   The store layer stays faithful and flags the PK violation;
   `publish_product` deduplicates with a documented rule (most complete row
   wins), preventing double-counted revenue in every downstream join.
2. **Partial OrderDates** — five orders (43828–43832) arrive as `YYYY-MM`.
   They are parsed as the 1st of the month with an `OrderDatePrecision`
   audit column; their year revenue is preserved for Q1, but their
   `LeadTimeInBusinessDays` is NULL rather than a fabricated ~24-day value.
3. **Discount semantics** — `UnitPriceDiscount` values (0–0.40) are clearly
   rates, but the specified formula treats them as amounts. Implemented
   exactly as specified; the discrepancy is documented as a client question
   (`docs/assumptions.md` §4).

Also preserved and reported: two `OrderQty = -1` return lines and 94
zero-price giveaway lines. All FK relationships validate with zero orphans;
`SalesPersonID` is NULL exactly on online orders (expected, not a defect).

## Key decisions (short version)

* **Business days** = Mon–Fri after `OrderDate` up to and **including**
  `ShipDate` (start-exclusive/end-inclusive): Mon→Fri = 4, Fri→Mon = 1,
  +7 calendar days = 5. Implemented UDF-free (`sequence`+`filter`+`size`)
  with a pure-Python reference tested for parity.
* **Money** = `DECIMAL(19,4)` (source carries 4 decimals; no float drift).
* **Size** stays STRING (mixed `38`–`70` and `S/M/L/XL` domain).
* **Join** = left from detail (defensive; equivalent to inner here — 0 orphans).

Full details: [`docs/data_model.md`](docs/data_model.md) (every column's type
and justification, key validation results) and
[`docs/assumptions.md`](docs/assumptions.md) (10 numbered assumptions and
trade-offs).

## Repository structure

```text
├── data/input/                  # the three delivered CSVs
├── src/
│   ├── config.py                # paths, Spark session — the only config spot
│   ├── main.py                  # single entry point (python -m src.main)
│   ├── raw/load_raw.py          # CSV → raw_* parquet (strings, as-is)
│   ├── store/
│   │   ├── schemas.py           # target types, PKs, FKs
│   │   ├── transform_store.py   # normalisation + ANSI-safe typing
│   │   └── quality_checks.py    # PK/FK/null/cast/anomaly report
│   ├── publish/
│   │   ├── publish_product.py   # dedupe, Color N/A, category enrichment
│   │   └── publish_orders.py    # join, lead time, extended price
│   └── utils/date_utils.py      # business-day logic (pure fn + Spark column)
├── analysis/analysis_questions.py
├── tests/                       # 22 tests; date logic tested hardest
└── docs/                        # data model + assumptions
```

## Testing

`python -m pytest tests/ -q` → **22 passed**. 
Coverage focuses on the risky logic: 13 parametrised business-day cases plus the 7-calendar-day rule across every starting weekday and a Spark↔Python parity sweep; 
Category-mapping rules including the `Frames` substring and the `Bib-Shorts ≠ Shorts` exact-match edge; 
Deduplication determinism: the publish_orders column contract, grain, rename, and derived-column values (including the month-precision NULL case).
