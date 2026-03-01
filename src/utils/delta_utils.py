"""Reusable Delta Lake utilities: MERGE, OPTIMIZE, VACUUM."""

from __future__ import annotations

import os
from typing import List

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def merge_into_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: List[str],
    update_columns: List[str] | None = None,
) -> None:
    """Upsert *source_df* into the Delta table at *target_path*.

    On the first run (table does not yet exist) the source DataFrame is written
    directly as a new Delta table.  On subsequent runs a standard MERGE INTO is
    performed: matched rows are updated, unmatched rows are inserted.

    Args:
        spark: Active SparkSession.
        source_df: DataFrame containing rows to merge.
        target_path: Filesystem path of the target Delta table.
        merge_keys: Column names used to join source and target (e.g. ["transaction_id"]).
        update_columns: Columns to update on match.  When *None* all columns are updated.
    """
    if not DeltaTable.isDeltaTable(spark, target_path):
        # First run — write directly; the caller is responsible for partitioning.
        source_df.write.format("delta").mode("overwrite").save(target_path)
        return

    target_table = DeltaTable.forPath(spark, target_path)

    merge_condition = " AND ".join(
        [f"target.{key} = source.{key}" for key in merge_keys]
    )

    if update_columns is None:
        update_set = {col: f"source.{col}" for col in source_df.columns}
    else:
        update_set = {col: f"source.{col}" for col in update_columns}

    insert_set = {col: f"source.{col}" for col in source_df.columns}

    (
        target_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_set)
        .execute()
    )


def optimize_table(
    spark: SparkSession,
    table_path: str,
    zorder_columns: List[str] | None = None,
) -> None:
    """Run OPTIMIZE (and optionally ZORDER BY) on a Delta table.

    Args:
        spark: Active SparkSession.
        table_path: Filesystem path of the Delta table.
        zorder_columns: Columns to pass to ZORDER BY.  Skipped when empty / None.
    """
    optimize_sql = f"OPTIMIZE delta.`{table_path}`"
    if zorder_columns:
        zorder_clause = ", ".join(zorder_columns)
        optimize_sql += f" ZORDER BY ({zorder_clause})"
    spark.sql(optimize_sql)


def vacuum_table(
    spark: SparkSession,
    table_path: str,
    retention_hours: int = 168,
) -> None:
    """Run VACUUM on a Delta table to remove old file versions.

    Args:
        spark: Active SparkSession.
        table_path: Filesystem path of the Delta table.
        retention_hours: Hours of history to retain (default 7 days).
    """
    spark.sql(
        f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS"
    )
