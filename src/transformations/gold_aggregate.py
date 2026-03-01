"""Gold layer aggregations: daily sales by region and by category."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, sum as spark_sum

from src.config import GOLD_CATEGORY_PATH, GOLD_REGION_PATH, SILVER_PATH
from src.utils.delta_utils import optimize_table

_DELTA_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.enableChangeDataFeed": "true",
}


def _set_table_properties(spark: SparkSession, path: str) -> None:
    """Apply Delta table properties via ALTER TABLE SQL."""
    props = ", ".join(
        f"'{k}' = '{v}'" for k, v in _DELTA_PROPERTIES.items()
    )
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ({props})")


def _get_valid_silver(spark: SparkSession):
    """Read silver table filtering to valid rows only.

    Args:
        spark: Active SparkSession.

    Returns:
        DataFrame of valid silver rows.
    """
    return (
        spark.read.format("delta")
        .load(SILVER_PATH)
        .filter(col("_is_valid") == True)  # noqa: E712
    )


def _replaceWhere_dates(df) -> str:
    """Build a replaceWhere predicate covering all event_dates in *df*."""
    dates = [row.event_date for row in df.select("event_date").distinct().collect()]
    if not dates:
        return "1=0"
    date_literals = ", ".join(f"DATE '{d}'" for d in dates)
    return f"event_date IN ({date_literals})"


def aggregate_gold(spark: SparkSession) -> None:
    """Compute both gold aggregations and write with partition-safe replaceWhere.

    Reads only ``_is_valid = true`` rows from silver, then produces:
    - ``daily_sales_by_region``: revenue / transactions / units / avg_discount grouped
      by event_date + region.
    - ``daily_sales_by_category``: revenue / transactions / units / avg_unit_price
      grouped by event_date + category.

    Each table is written using ``replaceWhere`` to overwrite only the affected
    event_date partitions, then OPTIMIZE + ZORDER is run.

    Args:
        spark: Active SparkSession.
    """
    valid_df = _get_valid_silver(spark)

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
        .save(GOLD_REGION_PATH)
    )
    _set_table_properties(spark, GOLD_REGION_PATH)
    optimize_table(spark, GOLD_REGION_PATH, zorder_columns=["region"])

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
        .save(GOLD_CATEGORY_PATH)
    )
    _set_table_properties(spark, GOLD_CATEGORY_PATH)
    optimize_table(spark, GOLD_CATEGORY_PATH, zorder_columns=["category"])
