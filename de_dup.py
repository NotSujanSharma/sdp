from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

class DeleteStrategy(Enum):
    NONE   = "none"   # No deletes, ignore
    DELETE = "delete" # delete_col/delete_value identify delete rows


@dataclass
class SequenceCol:
    name:       str
    descending: bool = True


class Deduplicator:
    _DEFAULT_DELETE_COL   = "_change_type"
    _DEFAULT_DELETE_VALUE = "delete"

    def __init__(
        self,
        primary_keys:    list[str],
        sequence_cols:   list[SequenceCol],
        delete_strategy: DeleteStrategy = DeleteStrategy.NONE,
        delete_col:      Optional[str]  = None,
        delete_value:    object         = None,
    ):
        self._primary_keys    = primary_keys
        self._sequence_cols   = sequence_cols
        self._delete_strategy = delete_strategy
        self._delete_col      = delete_col   if delete_col   is not None else self._DEFAULT_DELETE_COL
        self._delete_value    = delete_value if delete_value is not None else self._DEFAULT_DELETE_VALUE
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
    def with_delete(
        cls,
        primary_keys: list[str],
        sequence_by:  str,
        delete_col:   Optional[str] = None,
        delete_value: object        = None,
    ) -> "Deduplicator":
        return cls(
            primary_keys    = primary_keys,
            sequence_cols   = [SequenceCol(sequence_by, descending=True)],
            delete_strategy = DeleteStrategy.DELETE,
            delete_col      = delete_col,
            delete_value    = delete_value,
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

        window = self._build_window(upsert_df)

        deduped_df = (
            upsert_df
            .filter(F.row_number().over(window) == 1)
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

        if self._delete_strategy == DeleteStrategy.DELETE:
            delete_condition = F.col(self._delete_col) == F.lit(self._delete_value)
        else:
            raise ValueError(f"Unknown delete_strategy: {self._delete_strategy}")

        return df.filter(delete_condition), df.filter(~delete_condition)

    def _build_window(self, df: DataFrame) -> Window:
        window = Window.partitionBy(*self._primary_keys)

        order_exprs = []
        for seq_col in self._sequence_cols:
            col_expr = F.col(seq_col.name)
            if seq_col.descending:
                ordered = col_expr.desc_nulls_last()
            else:
                ordered = col_expr.asc_nulls_last()
            order_exprs.append(ordered)

        return window.orderBy(*order_exprs)

    def _validate(self) -> None:
        if not self._primary_keys:
            raise ValueError("primary_keys cannot be empty.")
        if not self._sequence_cols:
            raise ValueError("sequence_cols cannot be empty. Provide at least one SequenceCol.")


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


def dedup_with_delete(
    df:           DataFrame,
    primary_keys: list[str],
    sequence_by:  str,
    delete_col:   Optional[str] = None,
    delete_value: object        = None,
) -> DataFrame:
    return Deduplicator.with_delete(
        primary_keys, sequence_by, delete_col, delete_value
    ).run(df)