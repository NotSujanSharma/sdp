"""
transform_helpers.py  —  Pure DataFrame transformation helpers for Lakeflow SDP.

IMPORTANT (Lakeflow SDP rules):
  • This module contains ONLY functions that accept a DataFrame and return a DataFrame.
  • No spark.write / spark.save / display() calls — Lakeflow handles materialisation.
  • No side effects; functions may be called multiple times by the planner.
  • Import and call these helpers from within @dp.table / @dp.materialized_view functions.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, FloatType, IntegerType, LongType,
    StringType, TimestampType,
)

# ── Type registry ────────────────────────────────────────────────────────────
_SPARK_TYPES: dict[str, Any] = {
    "integer":   IntegerType(),
    "int":       IntegerType(),
    "long":      LongType(),
    "bigint":    LongType(),
    "double":    DoubleType(),
    "float":     FloatType(),
    "string":    StringType(),
    "timestamp": TimestampType(),
}


# ── Per-transformation handlers ──────────────────────────────────────────────
def _rename(df: DataFrame, t: dict) -> DataFrame:
    return df.withColumnRenamed(t["source_col"], t["target_col"])


def _cast(df: DataFrame, t: dict) -> DataFrame:
    spark_type = _SPARK_TYPES.get(t["target_type"].lower())
    if not spark_type:
        raise ValueError(f"Unsupported target_type: {t['target_type']}")
    return df.withColumn(t["column"], F.col(t["column"]).cast(spark_type))


def _upper(df: DataFrame, t: dict) -> DataFrame:
    return df.withColumn(t["column"], F.upper(F.col(t["column"])))


def _lower(df: DataFrame, t: dict) -> DataFrame:
    return df.withColumn(t["column"], F.lower(F.col(t["column"])))


def _trim(df: DataFrame, t: dict) -> DataFrame:
    return df.withColumn(t["column"], F.trim(F.col(t["column"])))


def _replace_null(df: DataFrame, t: dict) -> DataFrame:
    return df.withColumn(
        t["column"],
        F.coalesce(F.col(t["column"]), F.lit(t["value"])),
    )


def _derived(df: DataFrame, t: dict) -> DataFrame:
    return df.withColumn(t["column"], F.expr(t["expression"]))


def _add_audit_cols(df: DataFrame, _t: dict) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_layer", F.lit("bronze"))
    )


# ── Dispatcher ───────────────────────────────────────────────────────────────
_HANDLERS = {
    "rename":        _rename,
    "cast":          _cast,
    "upper":         _upper,
    "lower":         _lower,
    "trim":          _trim,
    "replace_null":  _replace_null,
    "derived":       _derived,
    "add_audit_cols": _add_audit_cols,
}


def apply_transformations(df: DataFrame, transformations: list[dict]) -> DataFrame:
    """
    Apply a list of metadata-driven transformations to *df* and return
    the resulting DataFrame.  Always add audit columns as the final step.
    """
    for t in transformations:
        kind = t["type"].lower()
        handler = _HANDLERS.get(kind)
        if not handler:
            raise ValueError(f"Unknown transformation type: '{kind}'")
        df = handler(df, t)

    # Always stamp audit columns
    df = _add_audit_cols(df, {})
    return df
