"""Bronze layer ingestion: read raw CSVs and append to Delta or Iceberg table."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, to_date

from src.config import BRONZE_PATH, ICEBERG_BRONZE_TABLE, SOURCE_PATH
from src.utils.iceberg_utils import iceberg_table_exists
from src.utils.schema_utils import SOURCE_SCHEMA

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


def ingest_bronze(
    spark: SparkSession,
    source_path: str = SOURCE_PATH,
    output_path: str = BRONZE_PATH,
    iceberg_table: str = ICEBERG_BRONZE_TABLE,
    format: str = "delta",
) -> int:
    """Read all CSV files from *source_path* and append raw rows to the bronze table.

    Metadata columns added:
    - ``_ingested_at``: timestamp of ingestion (current_timestamp)
    - ``_source_file``: path of the originating CSV file (input_file_name)
    - ``ingestion_date``: date derived from ``_ingested_at`` — used for partitioning

    Args:
        spark: Active SparkSession.
        source_path: Directory containing source CSV files. Defaults to ``SOURCE_PATH``.
        output_path: Destination Delta table path. Defaults to ``BRONZE_PATH``.
            Ignored when *format* is ``"iceberg"``.
        iceberg_table: Fully-qualified Iceberg table name. Defaults to ``ICEBERG_BRONZE_TABLE``.
            Ignored when *format* is ``"delta"``.
        format: ``"delta"`` (default) or ``"iceberg"``.

    Returns:
        Number of rows written to the bronze table in this run.
    """
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(SOURCE_SCHEMA)
        .load(source_path)
    )

    bronze_df = raw_df.withColumn(
        "_ingested_at", current_timestamp()
    ).withColumn(
        "_source_file", input_file_name()
    ).withColumn(
        "ingestion_date", to_date("_ingested_at")
    )

    row_count = bronze_df.count()

    if format == "iceberg":
        if not iceberg_table_exists(spark, iceberg_table):
            (
                bronze_df.writeTo(iceberg_table)
                .using("iceberg")
                .partitionedBy("ingestion_date")
                .create()
            )
        else:
            bronze_df.writeTo(iceberg_table).append()
    else:
        (
            bronze_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("ingestion_date")
            .save(output_path)
        )
        _set_delta_table_properties(spark, output_path)

    return row_count
