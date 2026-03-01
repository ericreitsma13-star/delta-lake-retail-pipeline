"""Silver layer transformation: cleanse, validate, deduplicate, and upsert."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    rank,
    to_date,
    trim,
    when,
)
from pyspark.sql.window import Window

from src.config import BRONZE_PATH, SILVER_PATH
from src.utils.delta_utils import merge_into_delta

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


def transform_silver(
    spark: SparkSession,
    bronze_path: str = BRONZE_PATH,
    silver_path: str = SILVER_PATH,
) -> dict:
    """Read bronze, apply quality rules, deduplicate, and merge into silver.

    Transformations applied:
    1. Cast ``event_date`` string → DateType.
    2. Set ``_is_valid = false`` where ``customer_id`` is null/empty or
       ``total_amount`` <= 0.
    3. Deduplicate on ``transaction_id`` keeping the row with the latest
       ``_ingested_at`` (window rank = 1).
    4. Add ``_updated_at = current_timestamp()``.
    5. MERGE INTO silver on ``transaction_id``.

    Args:
        spark: Active SparkSession.
        bronze_path: Source Delta table path. Defaults to ``BRONZE_PATH``.
        silver_path: Target Delta table path. Defaults to ``SILVER_PATH``.

    Returns:
        Dict with keys ``rows_read`` and ``rows_merged``.
    """
    bronze_df = spark.read.format("delta").load(bronze_path)
    rows_read = bronze_df.count()

    # Cast event_date from string to DateType
    silver_df = bronze_df.withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd"))

    # Validation flag
    customer_valid = col("customer_id").isNotNull() & (trim(col("customer_id")) != "")
    amount_valid = col("total_amount") > 0
    silver_df = silver_df.withColumn(
        "_is_valid", customer_valid & amount_valid
    )

    # Deduplicate: keep latest _ingested_at per transaction_id
    window_spec = Window.partitionBy("transaction_id").orderBy(col("_ingested_at").desc())
    silver_df = (
        silver_df.withColumn("_rank", rank().over(window_spec))
        .filter(col("_rank") == 1)
        .drop("_rank")
    )

    # Add _updated_at audit column
    silver_df = silver_df.withColumn("_updated_at", current_timestamp())

    rows_merged = silver_df.count()

    merge_into_delta(
        spark=spark,
        source_df=silver_df,
        target_path=silver_path,
        merge_keys=["transaction_id"],
    )

    _set_table_properties(spark, silver_path)

    return {"rows_read": rows_read, "rows_merged": rows_merged}
