# Databricks notebook source
# ─────────────────────────────────────────────────────────────────────────────
#  bronze_to_silver_pipeline.py
#
#  Lakeflow Spark Declarative Pipeline  —  Bronze → Silver
#
#  How to deploy:
#    1. Upload this file + transform_helpers.py + pipeline_config.json
#       into a Databricks Workspace folder.
#    2. Create a new Lakeflow Pipeline (Workflows → Delta Live Tables).
#    3. Set Source Code → point to this file.
#    4. Set Target Catalog = main, Target Schema = silver.
#    5. Click "Start".
#
#  What Lakeflow handles FOR YOU (no manual code needed):
#    ✅  Dependency graph & execution order
#    ✅  Incremental / streaming state & checkpointing
#    ✅  Deduplication via AUTO CDC SEQUENCE BY
#    ✅  Merge (upsert) into Silver Delta tables
#    ✅  DQ metrics dashboard in the Pipeline UI
#    ✅  Auto-retry on transient failures
#    ✅  Lineage graph visualization
# ─────────────────────────────────────────────────────────────────────────────

import json
from pathlib import Path

from pyspark import pipelines as dp   # Lakeflow SDP module  (replaces old `dlt`)
from pyspark.sql import functions as F

from transform_helpers import apply_transformations

# ── Load metadata config ─────────────────────────────────────────────────────
# Place pipeline_config.json in the same workspace folder as this notebook.
_CONFIG_PATH = Path("/Workspace/Shared/bronze_to_silver/pipeline_config.json")
with _CONFIG_PATH.open() as _f:
    PIPELINE_CONFIG = json.load(_f)

_TABLE_CONFIGS: dict[str, dict] = {
    t["table_name"]: t for t in PIPELINE_CONFIG["tables"]
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: attach @dp.expect / @dp.expect_or_drop / @dp.expect_or_fail
#         decorators programmatically from metadata config.
# ─────────────────────────────────────────────────────────────────────────────
def _apply_expectations(fn, expectations: list[dict]):
    """
    Wraps a pipeline function with Lakeflow expectation decorators
    derived from the metadata config.

    action = "drop"  → @dp.expect_or_drop   (bad rows silently removed)
    action = "fail"  → @dp.expect_or_fail   (pipeline halts on any violation)
    action = "warn"  → @dp.expect           (violation logged, row kept)
    """
    for exp in reversed(expectations):   # reversed so outer decorator = first in list
        name       = exp["name"]
        constraint = exp["constraint"]
        action     = exp.get("action", "warn").lower()

        if action == "drop":
            fn = dp.expect_or_drop(name, constraint)(fn)
        elif action == "fail":
            fn = dp.expect_or_fail(name, constraint)(fn)
        else:
            fn = dp.expect(name, constraint)(fn)

    return fn


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — BRONZE STAGING (Streaming Tables)
#
#  • Read raw Bronze tables as a stream (Auto Loader or Delta CDF).
#  • Each Bronze staging table is a PRIVATE streaming table — internal to the
#    pipeline, not exposed in the catalog.
#  • Lakeflow tracks watermarks and checkpoints automatically.
# ─────────────────────────────────────────────────────────────────────────────

# ── customers_bronze_raw ─────────────────────────────────────────────────────
@dp.table(name="customers_bronze_raw", comment="Raw streaming ingest from Bronze customers")
def customers_bronze_raw():
    """
    Stream from the Bronze customers Delta table.
    Using readStream + Delta source gives incremental processing —
    only new/changed rows are picked up on each pipeline run.
    """
    cfg = _TABLE_CONFIGS["customers"]
    return spark.readStream.table(cfg["bronze_table"])


# ── orders_bronze_raw ────────────────────────────────────────────────────────
@dp.table(name="orders_bronze_raw", comment="Raw streaming ingest from Bronze orders")
def orders_bronze_raw():
    cfg = _TABLE_CONFIGS["orders"]
    return spark.readStream.table(cfg["bronze_table"])


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — SILVER STAGING (Transformed + Validated Streaming Tables)
#
#  • Apply metadata-driven column transformations.
#  • Lakeflow @dp.expect_or_drop decorators enforce DQ rules:
#      - Rows failing "drop" rules are removed and counted in the DQ dashboard.
#      - Rows failing "warn" rules are flagged but kept.
#  • These are PRIVATE streaming tables (not published to catalog).
#    They feed into the AUTO CDC flow in Step 3.
# ─────────────────────────────────────────────────────────────────────────────

def _make_silver_staging(table_name: str):
    """
    Factory: returns a Lakeflow pipeline function for the silver staging table
    of *table_name*, with all expectations attached from metadata config.
    """
    cfg          = _TABLE_CONFIGS[table_name]
    staging_name = f"{table_name}_silver_staging"
    source_name  = f"{table_name}_bronze_raw"
    transforms   = cfg.get("transformations", [])
    expectations = cfg.get("expectations", [])

    # Define the core pipeline function
    def _pipeline_fn():
        return apply_transformations(
            spark.readStream.table(source_name),
            transforms,
        )

    # Attach expectations from metadata
    _pipeline_fn = _apply_expectations(_pipeline_fn, expectations)

    # Register as a Lakeflow streaming table
    _pipeline_fn = dp.table(
        name=staging_name,
        comment=f"Transformed + validated staging for silver.{table_name}",
    )(_pipeline_fn)

    return _pipeline_fn


# Materialise staging tables for each configured table
customers_silver_staging = _make_silver_staging("customers")
orders_silver_staging    = _make_silver_staging("orders")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — SILVER TABLES with AUTO CDC (Deduplication + Upsert)
#
#  This is the KEY step that replaces manual MERGE + dedup logic.
#
#  dp.create_streaming_table()  →  declares the target Silver table.
#  dp.create_auto_cdc_flow()    →  Lakeflow CDC flow that:
#      • Reads the staging stream
#      • Deduplicates by KEYS (primary key columns)
#      • Uses SEQUENCE BY to keep the LATEST record per key
#        (handles out-of-order arrivals automatically)
#      • Merges (upserts) into the Silver table
#      • Supports SCD Type 1 (default) or SCD Type 2 (with stored_as_scd_type=2)
#
#  No manual MERGE INTO, no ROW_NUMBER() window — Lakeflow owns all of that.
# ─────────────────────────────────────────────────────────────────────────────

def _register_silver_table(table_name: str):
    """
    Declares the Silver streaming table and wires the AUTO CDC flow
    from the staging table, all driven by metadata config.
    """
    cfg          = _TABLE_CONFIGS[table_name]
    silver_name  = f"silver_{table_name}"          # published as main.silver.<name>
    staging_name = f"{table_name}_silver_staging"
    primary_keys = cfg["primary_keys"]
    sequence_by  = cfg["sequence_by"]
    partition_by = cfg.get("partition_by", [])

    # ── Declare the Silver target table ──────────────────────────────────────
    dp.create_streaming_table(
        name         = silver_name,
        comment      = f"Silver table for {table_name} — deduplicated & upserted via AUTO CDC",
        partition_cols = partition_by if partition_by else None,
        table_properties = {
            "quality":                "silver",
            "delta.enableChangeDataFeed": "true",    # enables downstream CDF reads
        },
    )

    # ── Wire AUTO CDC flow ────────────────────────────────────────────────────
    # SCD Type 1: keeps only latest record per key (dedup + upsert)
    # Change to stored_as_scd_type=2 to keep full history.
    dp.create_auto_cdc_flow(
        name             = f"{table_name}_cdc_flow",
        target           = silver_name,
        source           = staging_name,
        keys             = primary_keys,
        sequence_by      = F.col(sequence_by),
        stored_as_scd_type = 1,                      # SCD1 = deduplicated latest state
        comment          = (
            f"AUTO CDC: dedup + upsert {table_name} "
            f"keyed on {primary_keys}, sequenced by {sequence_by}"
        ),
    )


# Register Silver tables for all configured tables
for _tbl in PIPELINE_CONFIG["tables"]:
    _register_silver_table(_tbl["table_name"])
