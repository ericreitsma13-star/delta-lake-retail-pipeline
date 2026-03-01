"""SparkSession singleton and path/table constants for the Delta Lake retail pipeline."""

from __future__ import annotations

import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

_spark: SparkSession | None = None

# ---------------------------------------------------------------------------
# Path constants (relative to /app inside Docker; adjust BASE_DIR for local)
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

SOURCE_PATH: str = os.path.join(DATA_DIR, "source")
SOURCE_PRODUCTS_PATH: str = os.path.join(DATA_DIR, "source", "products.parquet")
BRONZE_PATH: str = os.path.join(DATA_DIR, "bronze", "sales_transactions")
BRONZE_PRODUCTS_PATH: str = os.path.join(DATA_DIR, "bronze", "products")
SILVER_PATH: str = os.path.join(DATA_DIR, "silver", "sales_transactions")
SILVER_ENRICHED_PATH: str = os.path.join(DATA_DIR, "silver", "transactions_enriched")
GOLD_REGION_PATH: str = os.path.join(DATA_DIR, "gold", "daily_sales_by_region")
GOLD_CATEGORY_PATH: str = os.path.join(DATA_DIR, "gold", "daily_sales_by_category")
GOLD_MARGIN_PATH: str = os.path.join(DATA_DIR, "gold", "daily_margin_by_category")
STAGING_TRANSACTIONS_PATH: str = os.path.join(DATA_DIR, "staging", "transactions")

# ---------------------------------------------------------------------------
# Delta table name constants
# ---------------------------------------------------------------------------
BRONZE_TABLE: str = "bronze_sales_transactions"
SILVER_TABLE: str = "silver_sales_transactions"
GOLD_REGION_TABLE: str = "gold_daily_sales_by_region"
GOLD_CATEGORY_TABLE: str = "gold_daily_sales_by_category"

# ---------------------------------------------------------------------------
# Iceberg path and catalog constants
# ---------------------------------------------------------------------------
ICEBERG_WAREHOUSE: str = os.path.join(DATA_DIR, "iceberg")
ICEBERG_DB: str = "retail"
ICEBERG_BRONZE_TABLE: str = "iceberg.retail.bronze_sales_transactions"
ICEBERG_SILVER_TABLE: str = "iceberg.retail.silver_sales_transactions"
ICEBERG_GOLD_REGION_TABLE: str = "iceberg.retail.gold_daily_sales_by_region"
ICEBERG_GOLD_CATEGORY_TABLE: str = "iceberg.retail.gold_daily_sales_by_category"

_ICEBERG_PACKAGE: str = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"


def get_spark_session(app_name: str = "RetailPipeline") -> SparkSession:
    """Return (or create) the singleton SparkSession with Delta Lake and Iceberg extensions.

    Configures:
    - Delta Lake via ``configure_spark_with_delta_pip`` (downloads JAR at startup)
    - Apache Iceberg via ``spark.jars.packages`` (downloaded alongside Delta)
    - A named ``iceberg`` catalog backed by the local ``./data/iceberg/`` warehouse

    Args:
        app_name: Spark application name shown in the UI.

    Returns:
        A configured SparkSession instance.
    """
    global _spark
    if _spark is None:
        builder = (
            SparkSession.builder.appName(app_name)
            .config(
                "spark.sql.extensions",
                (
                    "io.delta.sql.DeltaSparkSessionExtension,"
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                ),
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.databricks.delta.retentionDurationCheck.enabled",
                "false",
            )
            # Iceberg named catalog (runs alongside Delta's spark_catalog)
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "hadoop")
            .config("spark.sql.catalog.iceberg.warehouse", ICEBERG_WAREHOUSE)
        )
        _spark = configure_spark_with_delta_pip(
            builder,
            extra_packages=[_ICEBERG_PACKAGE],
        ).getOrCreate()
        # Ensure the retail namespace exists in the Iceberg catalog
        _spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{ICEBERG_DB}")
    return _spark
