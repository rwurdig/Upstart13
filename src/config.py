"""Central configuration for the AdventureWorks pipeline.

All paths are resolved relative to the repository root so the pipeline can be
executed from any working directory with `python -m src.main`.
"""
from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = REPO_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

RAW_DIR = OUTPUT_DIR / "raw"          # raw_*     : as-delivered, all strings
STORE_DIR = OUTPUT_DIR / "store"      # store_*   : typed, faithful row counts
PUBLISH_DIR = OUTPUT_DIR / "publish"  # publish_* : business-ready
ANALYSIS_DIR = OUTPUT_DIR / "analysis"
QUALITY_REPORT_PATH = OUTPUT_DIR / "quality_report.md"

# Logical name -> input file
INPUT_FILES: dict[str, Path] = {
    "products": INPUT_DIR / "products.csv",
    "sales_order_header": INPUT_DIR / "sales-order-header.csv",
    "sales_order_detail": INPUT_DIR / "sales-order-detail.csv",
}

TABLES = list(INPUT_FILES)

# ---------------------------------------------------------------------------
# CSV read options (raw layer). Schema inference is deliberately disabled:
# the raw layer must preserve the data exactly as delivered (strings only).
# ---------------------------------------------------------------------------
CSV_OPTIONS: dict[str, str] = {
    "header": "true",
    "sep": ",",
    "encoding": "UTF-8",
    "inferSchema": "false",
}

APP_NAME = "adventureworks-pipeline"


def get_spark():
    """Create (or fetch) the SparkSession used by every pipeline stage."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(APP_NAME)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def raw_path(name: str) -> str:
    return str(RAW_DIR / f"raw_{name}")


def store_path(name: str) -> str:
    return str(STORE_DIR / f"store_{name}")


def publish_path(name: str) -> str:
    return str(PUBLISH_DIR / f"publish_{name}")
