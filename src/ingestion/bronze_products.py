"""Bronze layer ingestion for the products dimension table.

Reads the products Parquet file and writes a full-overwrite Delta table.
This is a dimension table so full overwrite (not append) is correct.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from src.config import BRONZE_PRODUCTS_PATH, SOURCE_PRODUCTS_PATH
from src.utils.schema_utils import PRODUCTS_SCHEMA

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


def ingest_bronze_products(
    spark: SparkSession,
    source_path: str = SOURCE_PRODUCTS_PATH,
    output_path: str = BRONZE_PRODUCTS_PATH,
) -> int:
    """Read the products Parquet file and overwrite the bronze products Delta table.

    A full overwrite is used because this is a dimension/reference table —
    the latest snapshot always replaces the previous one.

    Metadata columns added:
    - ``_ingested_at``: timestamp of ingestion (current_timestamp)

    Args:
        spark: Active SparkSession.
        source_path: Path to the products Parquet file. Defaults to ``SOURCE_PRODUCTS_PATH``.
        output_path: Destination Delta table path. Defaults to ``BRONZE_PRODUCTS_PATH``.

    Returns:
        Number of product rows written.
    """
    df = (
        spark.read.format("parquet")
        .schema(PRODUCTS_SCHEMA)
        .load(source_path)
    )

    bronze_df = df.withColumn("_ingested_at", current_timestamp())

    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .save(output_path)
    )
    _set_delta_table_properties(spark, output_path)

    return bronze_df.count()
