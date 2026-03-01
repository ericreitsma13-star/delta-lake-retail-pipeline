"""Reusable Apache Iceberg utilities: table existence check, MERGE, and optimize."""

from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession


def iceberg_table_exists(spark: SparkSession, table_name: str) -> bool:
    """Return True if the fully-qualified Iceberg table exists in the catalog.

    Args:
        spark: Active SparkSession.
        table_name: Fully-qualified table name, e.g. ``iceberg.retail.silver_sales_transactions``.

    Returns:
        ``True`` if the table exists, ``False`` otherwise.
    """
    return spark.catalog.tableExists(table_name)


def merge_into_iceberg(
    spark: SparkSession,
    source_df: DataFrame,
    table_name: str,
    merge_keys: List[str],
    partition_cols: List[str] | None = None,
) -> None:
    """Upsert *source_df* into the Iceberg table *table_name*.

    On the first run (table does not yet exist) the source DataFrame is written
    directly as a new Iceberg table.  On subsequent runs a SQL MERGE INTO is
    performed: matched rows are updated (``UPDATE SET *``), unmatched rows are
    inserted (``INSERT *``).

    Args:
        spark: Active SparkSession.
        source_df: DataFrame containing rows to merge.
        table_name: Fully-qualified Iceberg table name (e.g. ``iceberg.retail.silver_sales_transactions``).
        merge_keys: Column names used to join source and target (e.g. ``["transaction_id"]``).
        partition_cols: Columns to partition by when creating the table on the first run.
            Ignored on subsequent runs.
    """
    if not iceberg_table_exists(spark, table_name):
        writer = source_df.writeTo(table_name).using("iceberg")
        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)
        writer.create()
        return

    source_df.createOrReplaceTempView("_iceberg_merge_source")
    condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING _iceberg_merge_source AS source
        ON {condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def optimize_iceberg_table(spark: SparkSession, table_name: str) -> None:
    """Compact small files in an Iceberg table using ``rewrite_data_files``.

    Args:
        spark: Active SparkSession.
        table_name: Fully-qualified Iceberg table name
            (e.g. ``iceberg.retail.gold_daily_sales_by_region``).
            The leading catalog segment (``iceberg.``) is stripped automatically
            when constructing the stored-procedure argument.
    """
    # Strip the leading catalog name to get the db.table reference expected by
    # the iceberg.system.rewrite_data_files procedure (e.g. "retail.table_name").
    parts = table_name.split(".", 1)
    db_table = parts[1] if len(parts) == 2 else table_name
    spark.sql(
        f"CALL iceberg.system.rewrite_data_files(table => '{db_table}')"
    )
