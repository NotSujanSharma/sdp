from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION DATACLASSES
# ─────────────────────────────────────────────────────────────────────────────

class NullPosition(Enum):
    """Where to rank NULL values in the sequence ordering."""
    FIRST = "first"   # NULLs win  (treated as highest sequence value)
    LAST  = "last"    # NULLs lose (treated as lowest sequence value)


class DeleteStrategy(Enum):
    """How to handle delete signals during deduplication."""
    NONE        = "none"        # No deletes, ignore
    SOFT_DELETE = "soft_delete" # is_deleted flag on the row
    HARD_DELETE = "hard_delete" # CDF _change_type = 'delete' column


@dataclass
class SequenceCol:
    """
    A single column used for ordering during deduplication.

    Args:
        name:        Column name to order by.
        descending:  True = highest value wins (default, use for timestamps).
                     False = lowest value wins.
        null_pos:    Where NULLs rank. Default = LAST (NULLs lose).
    """
    name:       str
    descending: bool          = True
    null_pos:   NullPosition  = NullPosition.LAST


@dataclass
class DeduplicatorConfig:
    """
    Full configuration for a deduplication run.

    Args:
        primary_keys:       Columns that together form the unique key.
                            Supports composite keys e.g. ["order_id", "line_item_id"]

        sequence_cols:      Ordered list of SequenceCol objects.
                            First col is primary sort, subsequent cols break ties.
                            At least one required.

        delete_strategy:    How to handle delete rows. Default = NONE.

        soft_delete_col:    Column name for soft delete flag.
                            Required when delete_strategy = SOFT_DELETE.

        soft_delete_value:  Value that means "deleted". Default = True.

        add_row_hash:       Whether to add _dedup_row_hash audit column.
                            Useful for detecting changes downstream. Default = True.
    """
    primary_keys:       list[str]
    sequence_cols:      list[SequenceCol]
    delete_strategy:    DeleteStrategy    = DeleteStrategy.NONE
    soft_delete_col:    Optional[str]     = None
    soft_delete_value:  object            = True
    add_row_hash:       bool              = True


# ─────────────────────────────────────────────────────────────────────────────
# DEDUPLICATOR
# ─────────────────────────────────────────────────────────────────────────────

class Deduplicator:
    """
    Modular deduplication engine for Lakeflow Materialized View pipelines.

    Handles:
      ✅ Single and composite primary keys
      ✅ Out-of-order stream arrivals
      ✅ Null sequence values (configurable win/lose)
      ✅ Tie-breaking via deterministic row hash
      ✅ Soft deletes (is_deleted flag)
      ✅ Hard deletes (CDF _change_type column)
      ✅ Multiple sequence columns (composite ordering)
      ✅ Audit column (_dedup_row_hash)

    Usage:
        dedup = Deduplicator(config)
        clean_df = dedup.run(df)

    Or use the shorthand constructor:
        dedup = Deduplicator.simple(
            primary_keys  = ["customer_id"],
            sequence_by   = "updated_at",
        )
        clean_df = dedup.run(df)
    """

    # Internal column names — unlikely to clash with real data
    _RANK_COL = "__dedup_rank__"
    _HASH_COL = "_dedup_row_hash"

    def __init__(self, config: DeduplicatorConfig):
        self._cfg = config
        self._validate_config()

    # ── Shorthand constructors ────────────────────────────────────────────────

    @classmethod
    def simple(
        cls,
        primary_keys: list[str],
        sequence_by:  str,
        descending:   bool = True,
    ) -> "Deduplicator":
        """
        Quickest way to create a Deduplicator for the most common case:
        single sequence column, no deletes.

        Example:
            dedup = Deduplicator.simple(["customer_id"], "updated_at")
        """
        return cls(DeduplicatorConfig(
            primary_keys  = primary_keys,
            sequence_cols = [SequenceCol(sequence_by, descending=descending)],
        ))

    @classmethod
    def with_soft_delete(
        cls,
        primary_keys:      list[str],
        sequence_by:       str,
        soft_delete_col:   str,
        soft_delete_value: object = True,
    ) -> "Deduplicator":
        """
        Deduplicator for tables that signal deletes via a flag column.

        Example:
            dedup = Deduplicator.with_soft_delete(
                primary_keys    = ["customer_id"],
                sequence_by     = "updated_at",
                soft_delete_col = "is_deleted",
            )
        """
        return cls(DeduplicatorConfig(
            primary_keys      = primary_keys,
            sequence_cols     = [SequenceCol(sequence_by, descending=True)],
            delete_strategy   = DeleteStrategy.SOFT_DELETE,
            soft_delete_col   = soft_delete_col,
            soft_delete_value = soft_delete_value,
        ))

    @classmethod
    def with_hard_delete(
        cls,
        primary_keys: list[str],
        sequence_by:  str,
    ) -> "Deduplicator":
        """
        Deduplicator for CDF streams where _change_type = 'delete' signals removal.

        Example:
            dedup = Deduplicator.with_hard_delete(
                primary_keys = ["customer_id"],
                sequence_by  = "updated_at",
            )
        """
        return cls(DeduplicatorConfig(
            primary_keys    = primary_keys,
            sequence_cols   = [SequenceCol(sequence_by, descending=True)],
            delete_strategy = DeleteStrategy.HARD_DELETE,
        ))

    @classmethod
    def composite(
        cls,
        primary_keys:  list[str],
        sequence_cols: list[SequenceCol],
    ) -> "Deduplicator":
        """
        Deduplicator with composite primary keys and multi-column ordering.

        Example:
            dedup = Deduplicator.composite(
                primary_keys  = ["order_id", "line_item_id"],
                sequence_cols = [
                    SequenceCol("updated_at",     descending=True),
                    SequenceCol("version_number", descending=True),
                ],
            )
        """
        return cls(DeduplicatorConfig(
            primary_keys  = primary_keys,
            sequence_cols = sequence_cols,
        ))

    # ── Main entry point ──────────────────────────────────────────────────────

    def run(self, df: DataFrame) -> DataFrame:
        """
        Run deduplication on the input DataFrame.
        Returns a deduplicated DataFrame — one row per unique primary key.

        Steps:
          1. Separate delete rows (if delete strategy configured)
          2. Add row hash for tie-breaking + audit
          3. Build window, assign rank
          4. Keep rank = 1 only
          5. Reattach delete rows
          6. Drop internal columns
        """
        cfg = self._cfg

        # ── 1. Separate deletes so they bypass ranking logic ──────────────────
        delete_df, upsert_df = self._split_deletes(df)

        # ── 2. Add deterministic row hash to upserts ──────────────────────────
        #       Used as final tiebreaker when all sequence cols are equal.
        #       Also useful downstream as a change-detection hash.
        if cfg.add_row_hash:
            upsert_df = self._add_row_hash(upsert_df)

        # ── 3. Build window spec from config ──────────────────────────────────
        window = self._build_window(upsert_df)

        # ── 4. Rank and filter ────────────────────────────────────────────────
        deduped_df = (
            upsert_df
            .withColumn(self._RANK_COL, F.row_number().over(window))
            .filter(F.col(self._RANK_COL) == 1)
            .drop(self._RANK_COL)
        )

        # ── 5. Reattach delete rows ───────────────────────────────────────────
        if delete_df is not None:
            deduped_df = deduped_df.unionByName(
                delete_df, allowMissingColumns=True
            )

        return deduped_df

    # ── Private helpers ───────────────────────────────────────────────────────

    def _split_deletes(
        self, df: DataFrame
    ) -> tuple[Optional[DataFrame], DataFrame]:
        """
        Splits the DataFrame into (delete_rows, upsert_rows).
        delete_rows bypasses ranking — they always pass through.
        Returns (None, df) when no delete strategy is configured.
        """
        cfg = self._cfg

        if cfg.delete_strategy == DeleteStrategy.NONE:
            return None, df

        if cfg.delete_strategy == DeleteStrategy.SOFT_DELETE:
            if not cfg.soft_delete_col:
                raise ValueError(
                    "soft_delete_col must be set when delete_strategy=SOFT_DELETE"
                )
            delete_condition = (
                F.col(cfg.soft_delete_col) == F.lit(cfg.soft_delete_value)
            )

        elif cfg.delete_strategy == DeleteStrategy.HARD_DELETE:
            # CDF standard column
            delete_condition = F.col("_change_type") == F.lit("delete")

        else:
            raise ValueError(f"Unknown delete_strategy: {cfg.delete_strategy}")

        delete_df  = df.filter(delete_condition)
        upsert_df  = df.filter(~delete_condition)
        return delete_df, upsert_df

    def _add_row_hash(self, df: DataFrame) -> DataFrame:
        """
        Adds _dedup_row_hash: an MD5 of all column values concatenated.
        Serves two purposes:
          1. Deterministic tiebreaker when sequence cols are equal
          2. Audit column — downstream can detect if a row actually changed
        """
        all_cols = df.columns
        hash_expr = F.md5(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                                 for c in all_cols])
        )
        return df.withColumn(self._HASH_COL, hash_expr)

    def _build_window(self, df: DataFrame) -> Window:
        """
        Builds the Window spec from config:
          PARTITION BY  primary_keys
          ORDER BY      sequence_cols (with null handling) + hash tiebreaker
        """
        cfg = self._cfg

        # PARTITION BY primary keys
        window = Window.partitionBy(*cfg.primary_keys)

        # ORDER BY each sequence column with correct direction + null handling
        order_exprs = []
        for seq_col in cfg.sequence_cols:
            col_expr = F.col(seq_col.name)

            if seq_col.descending:
                ordered = (
                    col_expr.desc_nulls_last()
                    if seq_col.null_pos == NullPosition.LAST
                    else col_expr.desc_nulls_first()
                )
            else:
                ordered = (
                    col_expr.asc_nulls_last()
                    if seq_col.null_pos == NullPosition.LAST
                    else col_expr.asc_nulls_first()
                )

            order_exprs.append(ordered)

        # Final tiebreaker: deterministic hash (so rank=1 is always the same row
        # even if pipeline re-runs with same data in different order)
        if cfg.add_row_hash and self._HASH_COL in df.columns:
            order_exprs.append(F.col(self._HASH_COL).asc())

        return window.orderBy(*order_exprs)

    def _validate_config(self) -> None:
        cfg = self._cfg
        if not cfg.primary_keys:
            raise ValueError("primary_keys cannot be empty.")
        if not cfg.sequence_cols:
            raise ValueError("sequence_cols cannot be empty. Provide at least one SequenceCol.")
        if cfg.delete_strategy == DeleteStrategy.SOFT_DELETE and not cfg.soft_delete_col:
            raise ValueError("soft_delete_col is required when delete_strategy=SOFT_DELETE.")


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE FUNCTIONS  —  for teams that prefer function calls over classes
# ─────────────────────────────────────────────────────────────────────────────

def dedup_simple(
    df:           DataFrame,
    primary_keys: list[str],
    sequence_by:  str,
    descending:   bool = True,
) -> DataFrame:
    """
    One-liner dedup for the most common case.

    Example:
        df = dedup_simple(df, ["customer_id"], "updated_at")
    """
    return Deduplicator.simple(primary_keys, sequence_by, descending).run(df)


def dedup_composite(
    df:            DataFrame,
    primary_keys:  list[str],
    sequence_cols: list[SequenceCol],
) -> DataFrame:
    """
    One-liner dedup for composite keys and multi-column ordering.

    Example:
        df = dedup_composite(
            df,
            primary_keys  = ["order_id", "line_item_id"],
            sequence_cols = [
                SequenceCol("updated_at",     descending=True),
                SequenceCol("version_number", descending=True),
            ],
        )
    """
    return Deduplicator.composite(primary_keys, sequence_cols).run(df)


def dedup_with_soft_delete(
    df:                DataFrame,
    primary_keys:      list[str],
    sequence_by:       str,
    soft_delete_col:   str,
    soft_delete_value: object = True,
) -> DataFrame:
    """
    One-liner dedup for tables with a soft delete flag column.

    Example:
        df = dedup_with_soft_delete(df, ["customer_id"], "updated_at", "is_deleted")
    """
    return Deduplicator.with_soft_delete(
        primary_keys, sequence_by, soft_delete_col, soft_delete_value
    ).run(df)


def dedup_with_hard_delete(
    df:           DataFrame,
    primary_keys: list[str],
    sequence_by:  str,
) -> DataFrame:
    """
    One-liner dedup for CDF streams with _change_type = 'delete' rows.

    Example:
        df = dedup_with_hard_delete(df, ["customer_id"], "updated_at")
    """
    return Deduplicator.with_hard_delete(primary_keys, sequence_by).run(df)
