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
BRONZE_PATH: str = os.path.join(DATA_DIR, "bronze", "sales_transactions")
SILVER_PATH: str = os.path.join(DATA_DIR, "silver", "sales_transactions")
GOLD_REGION_PATH: str = os.path.join(DATA_DIR, "gold", "daily_sales_by_region")
GOLD_CATEGORY_PATH: str = os.path.join(DATA_DIR, "gold", "daily_sales_by_category")

# ---------------------------------------------------------------------------
# Table name constants
# ---------------------------------------------------------------------------
BRONZE_TABLE: str = "bronze_sales_transactions"
SILVER_TABLE: str = "silver_sales_transactions"
GOLD_REGION_TABLE: str = "gold_daily_sales_by_region"
GOLD_CATEGORY_TABLE: str = "gold_daily_sales_by_category"


def get_spark_session(app_name: str = "RetailPipeline") -> SparkSession:
    """Return (or create) the singleton SparkSession with Delta Lake extensions.

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
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.databricks.delta.retentionDurationCheck.enabled",
                "false",
            )
        )
        _spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return _spark
