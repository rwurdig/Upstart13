"""Pipeline entry point.

Run from the repository root:

    python -m src.main

Stages: raw -> store (+ quality checks) -> publish -> analysis.
Every stage is idempotent (mode=overwrite), so the pipeline can be re-run
end-to-end at any time.
"""
from __future__ import annotations

import sys
import time

from src import config
from src.raw.load_raw import load_raw
from src.store.transform_store import build_store
from src.store.quality_checks import run_quality_checks
from src.publish.publish_product import build_publish_product
from src.publish.publish_orders import build_publish_orders
from analysis.analysis_questions import run_analysis


def main() -> int:
    started = time.time()
    spark = config.get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("AdventureWorks pipeline | raw -> store -> publish -> analysis")
    print("=" * 70)

    raw = load_raw(spark)
    store = build_store(raw)
    run_quality_checks(raw, store)

    product = build_publish_product(store["products"])
    orders = build_publish_orders(
        store["sales_order_detail"], store["sales_order_header"]
    )

    run_analysis(orders, product)

    print(f"\nDone in {time.time() - started:,.1f}s. "
          f"Outputs under {config.OUTPUT_DIR}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
