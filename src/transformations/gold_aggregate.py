"""Gold layer aggregations: daily sales by region and by category."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, sum as spark_sum

from src.config import (
    GOLD_CATEGORY_PATH,
    GOLD_REGION_PATH,
    ICEBERG_GOLD_CATEGORY_TABLE,
    ICEBERG_GOLD_REGION_TABLE,
    ICEBERG_SILVER_TABLE,
    SILVER_PATH,
)
from src.utils.delta_utils import optimize_table
from src.utils.iceberg_utils import merge_into_iceberg, optimize_iceberg_table

_DELTA_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.enableChangeDataFeed": "true",
}


def _set_delta_table_properties(spark: SparkSession, path: str) -> None:
    """Apply Delta table properties via ALTER TABLE SQL."""
    props = ", ".join(
        f"'{k}' = '{v}'" for k, v in _DELTA_PROPERTIES.items()
    )
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ({props})")


def _get_valid_silver_delta(spark: SparkSession, silver_path: str):
    """Read Delta silver table filtering to valid rows only."""
    return (
        spark.read.format("delta")
        .load(silver_path)
        .filter(col("_is_valid") == True)  # noqa: E712
    )


def _get_valid_silver_iceberg(spark: SparkSession, silver_table: str):
    """Read Iceberg silver table filtering to valid rows only."""
    return spark.table(silver_table).filter(col("_is_valid") == True)  # noqa: E712


def _replaceWhere_dates(df) -> str:
    """Build a replaceWhere predicate covering all event_dates in *df*."""
    dates = [row.event_date for row in df.select("event_date").distinct().collect()]
    if not dates:
        return "1=0"
    date_literals = ", ".join(f"DATE '{d}'" for d in dates)
    return f"event_date IN ({date_literals})"


def aggregate_gold(
    spark: SparkSession,
    silver_path: str = SILVER_PATH,
    region_path: str = GOLD_REGION_PATH,
    category_path: str = GOLD_CATEGORY_PATH,
    silver_table: str = ICEBERG_SILVER_TABLE,
    region_table: str = ICEBERG_GOLD_REGION_TABLE,
    category_table: str = ICEBERG_GOLD_CATEGORY_TABLE,
    format: str = "delta",
) -> None:
    """Compute both gold aggregations and write to Delta or Iceberg.

    Reads only ``_is_valid = true`` rows from silver, then produces:
    - ``daily_sales_by_region``: revenue / transactions / units / avg_discount grouped
      by event_date + region.
    - ``daily_sales_by_category``: revenue / transactions / units / avg_unit_price
      grouped by event_date + category.

    **Delta**: each table uses ``replaceWhere`` partition-safe overwrites + ZORDER.
    **Iceberg**: each table uses SQL MERGE INTO with composite merge keys + ``rewrite_data_files``.

    Args:
        spark: Active SparkSession.
        silver_path: Source Delta silver table path. Defaults to ``SILVER_PATH``.
            Ignored when *format* is ``"iceberg"``.
        region_path: Delta destination for daily_sales_by_region. Defaults to ``GOLD_REGION_PATH``.
            Ignored when *format* is ``"iceberg"``.
        category_path: Delta destination for daily_sales_by_category. Defaults to ``GOLD_CATEGORY_PATH``.
            Ignored when *format* is ``"iceberg"``.
        silver_table: Fully-qualified Iceberg silver table name.
            Ignored when *format* is ``"delta"``.
        region_table: Fully-qualified Iceberg region gold table name.
            Ignored when *format* is ``"delta"``.
        category_table: Fully-qualified Iceberg category gold table name.
            Ignored when *format* is ``"delta"``.
        format: ``"delta"`` (default) or ``"iceberg"``.
    """
    if format == "iceberg":
        valid_df = _get_valid_silver_iceberg(spark, silver_table)

        region_df = valid_df.groupBy("event_date", "region").agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("*").cast("integer").alias("total_transactions"),
            spark_sum("quantity").cast("integer").alias("total_units"),
            avg("discount_pct").alias("avg_discount_pct"),
        )
        merge_into_iceberg(
            spark=spark,
            source_df=region_df,
            table_name=region_table,
            merge_keys=["event_date", "region"],
            partition_cols=["event_date"],
        )
        optimize_iceberg_table(spark, region_table)

        category_df = valid_df.groupBy("event_date", "category").agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("*").cast("integer").alias("total_transactions"),
            spark_sum("quantity").cast("integer").alias("total_units"),
            avg("unit_price").alias("avg_unit_price"),
        )
        merge_into_iceberg(
            spark=spark,
            source_df=category_df,
            table_name=category_table,
            merge_keys=["event_date", "category"],
            partition_cols=["event_date"],
        )
        optimize_iceberg_table(spark, category_table)
        return

    # Delta path
    valid_df = _get_valid_silver_delta(spark, silver_path)

    # ------------------------------------------------------------------
    # daily_sales_by_region
    # ------------------------------------------------------------------
    region_df = valid_df.groupBy("event_date", "region").agg(
        spark_sum("total_amount").alias("total_revenue"),
        count("*").cast("integer").alias("total_transactions"),
        spark_sum("quantity").cast("integer").alias("total_units"),
        avg("discount_pct").alias("avg_discount_pct"),
    )

    region_replace_where = _replaceWhere_dates(region_df)

    (
        region_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", region_replace_where)
        .partitionBy("event_date")
        .save(region_path)
    )
    _set_delta_table_properties(spark, region_path)
    optimize_table(spark, region_path, zorder_columns=["region"])

    # ------------------------------------------------------------------
    # daily_sales_by_category
    # ------------------------------------------------------------------
    category_df = valid_df.groupBy("event_date", "category").agg(
        spark_sum("total_amount").alias("total_revenue"),
        count("*").cast("integer").alias("total_transactions"),
        spark_sum("quantity").cast("integer").alias("total_units"),
        avg("unit_price").alias("avg_unit_price"),
    )

    category_replace_where = _replaceWhere_dates(category_df)

    (
        category_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", category_replace_where)
        .partitionBy("event_date")
        .save(category_path)
    )
    _set_delta_table_properties(spark, category_path)
    optimize_table(spark, category_path, zorder_columns=["category"])
