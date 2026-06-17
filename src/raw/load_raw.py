"""Raw layer: load the delivered CSV files exactly as-is.

Every column is kept as a string (no schema inference, no cleaning) so the
raw_* tables are a faithful, replayable copy of the source files. All
typing/cleaning decisions happen downstream in the store layer, which keeps
them auditable against this layer.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src import config


def load_raw(spark: SparkSession) -> dict[str, DataFrame]:
    """Read each input CSV and persist it as a raw_* parquet table."""
    raw_frames: dict[str, DataFrame] = {}

    for name, path in config.INPUT_FILES.items():
        if not path.exists():
            raise FileNotFoundError(f"Expected input file is missing: {path}")

        df = spark.read.options(**config.CSV_OPTIONS).csv(str(path))
        df.write.mode("overwrite").parquet(config.raw_path(name))

        raw_frames[name] = spark.read.parquet(config.raw_path(name))
        print(f"[raw] raw_{name}: {raw_frames[name].count():,} rows, "
              f"{len(raw_frames[name].columns)} columns -> {config.raw_path(name)}")

    return raw_frames
