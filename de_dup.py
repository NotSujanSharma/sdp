from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


class NullPosition(Enum):
    FIRST = "first"   # treated as highest sequence value
    LAST  = "last"    # treated as lowest sequence value


class DeleteStrategy(Enum):
    NONE        = "none"        # No deletes, ignore
    SOFT_DELETE = "soft_delete" # is_deleted flag on the row
    HARD_DELETE = "hard_delete" # cdf _change_type = 'delete' column


@dataclass
class SequenceCol:
    name:       str
    descending: bool          = True
    null_pos:   NullPosition  = NullPosition.LAST


class Deduplicator:
    _RANK_COL = "__dedup_rank__"
    _HASH_COL = "_dedup_row_hash"

    def __init__(
        self,
        primary_keys:      list[str],
        sequence_cols:     list[SequenceCol],
        delete_strategy:   DeleteStrategy  = DeleteStrategy.NONE,
        soft_delete_col:   Optional[str]   = None,
        soft_delete_value: object          = True,
    ):
        self._primary_keys      = primary_keys
        self._sequence_cols     = sequence_cols
        self._delete_strategy   = delete_strategy
        self._soft_delete_col   = soft_delete_col
        self._soft_delete_value = soft_delete_value
        self._validate()


    @classmethod
    def simple(
        cls,
        primary_keys: list[str],
        sequence_by:  str,
        descending:   bool = True,
    ) -> "Deduplicator":
        return cls(
            primary_keys  = primary_keys,
            sequence_cols = [SequenceCol(sequence_by, descending=descending)],
        )

    @classmethod
    def with_soft_delete(
        cls,
        primary_keys:      list[str],
        sequence_by:       str,
        soft_delete_col:   str,
        soft_delete_value: object = True,
    ) -> "Deduplicator":
        return cls(
            primary_keys      = primary_keys,
            sequence_cols     = [SequenceCol(sequence_by, descending=True)],
            delete_strategy   = DeleteStrategy.SOFT_DELETE,
            soft_delete_col   = soft_delete_col,
            soft_delete_value = soft_delete_value,
        )

    @classmethod
    def with_hard_delete(
        cls,
        primary_keys: list[str],
        sequence_by:  str,
    ) -> "Deduplicator":
        return cls(
            primary_keys    = primary_keys,
            sequence_cols   = [SequenceCol(sequence_by, descending=True)],
            delete_strategy = DeleteStrategy.HARD_DELETE,
        )

    @classmethod
    def composite(
        cls,
        primary_keys:  list[str],
        sequence_cols: list[SequenceCol],
    ) -> "Deduplicator":
        return cls(
            primary_keys  = primary_keys,
            sequence_cols = sequence_cols,
        )


    def run(self, df: DataFrame) -> DataFrame:
        delete_df, upsert_df = self._split_deletes(df)

        upsert_df = self._add_row_hash(upsert_df)

        window = self._build_window(upsert_df)

        deduped_df = (
            upsert_df
            .withColumn(self._RANK_COL, F.row_number().over(window))
            .filter(F.col(self._RANK_COL) == 1)
            .drop(self._RANK_COL)
        )

        if delete_df is not None:
            deduped_df = deduped_df.unionByName(
                delete_df, allowMissingColumns=True
            )

        return deduped_df


    def _split_deletes(
        self, df: DataFrame
    ) -> tuple[Optional[DataFrame], DataFrame]:
        if self._delete_strategy == DeleteStrategy.NONE:
            return None, df

        if self._delete_strategy == DeleteStrategy.SOFT_DELETE:
            delete_condition = (
                F.col(self._soft_delete_col) == F.lit(self._soft_delete_value)
            )
        elif self._delete_strategy == DeleteStrategy.HARD_DELETE:
            delete_condition = F.col("_change_type") == F.lit("delete")
        else:
            raise ValueError(f"Unknown delete_strategy: {self._delete_strategy}")

        return df.filter(delete_condition), df.filter(~delete_condition)

    def _add_row_hash(self, df: DataFrame) -> DataFrame:
        all_cols = df.columns
        hash_expr = F.md5(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                                 for c in all_cols])
        )
        return df.withColumn(self._HASH_COL, hash_expr)

    def _build_window(self, df: DataFrame) -> Window:
        window = Window.partitionBy(*self._primary_keys)

        order_exprs = []
        for seq_col in self._sequence_cols:
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

        if self._HASH_COL in df.columns:
            order_exprs.append(F.col(self._HASH_COL).asc())

        return window.orderBy(*order_exprs)

    def _validate(self) -> None:
        if not self._primary_keys:
            raise ValueError("primary_keys cannot be empty.")
        if not self._sequence_cols:
            raise ValueError("sequence_cols cannot be empty. Provide at least one SequenceCol.")
        if self._delete_strategy == DeleteStrategy.SOFT_DELETE and not self._soft_delete_col:
            raise ValueError("soft_delete_col is required when using with_soft_delete().")


def dedup_simple(
    df:           DataFrame,
    primary_keys: list[str],
    sequence_by:  str,
    descending:   bool = True,
) -> DataFrame:
    return Deduplicator.simple(primary_keys, sequence_by, descending).run(df)


def dedup_composite(
    df:            DataFrame,
    primary_keys:  list[str],
    sequence_cols: list[SequenceCol],
) -> DataFrame:
    return Deduplicator.composite(primary_keys, sequence_cols).run(df)


def dedup_with_soft_delete(
    df:                DataFrame,
    primary_keys:      list[str],
    sequence_by:       str,
    soft_delete_col:   str,
    soft_delete_value: object = True,
) -> DataFrame:
    return Deduplicator.with_soft_delete(
        primary_keys, sequence_by, soft_delete_col, soft_delete_value
    ).run(df)


def dedup_with_hard_delete(
    df:           DataFrame,
    primary_keys: list[str],
    sequence_by:  str,
) -> DataFrame:
    return Deduplicator.with_hard_delete(primary_keys, sequence_by).run(df)