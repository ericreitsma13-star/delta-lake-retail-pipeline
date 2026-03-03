"""Retail pipeline DAG: orchestrates Bronze → Silver → Gold for Delta and Iceberg.

Task graph
----------

  ingest_bronze_products ─┬───────────────────────────────────┐
                          └─ ingest_bronze_delta ── transform_silver_delta
                               └─ enrich_silver ── aggregate_gold_delta
                                    └─ aggregate_gold_margin
                                         └─ ingest_bronze_iceberg
                                              └─ transform_silver_iceberg
                                                   └─ aggregate_gold_iceberg

The Iceberg branch runs after the Delta branch is fully complete to avoid
concurrent SparkSession conflicts on a single-node local setup.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="retail_pipeline",
    description="Delta + Iceberg retail medallion pipeline (Bronze → Silver → Gold)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["retail", "delta", "iceberg"],
)
def retail_pipeline():
    """Full pipeline: products ingestion, Delta branch, then Iceberg branch."""

    # ------------------------------------------------------------------
    # Shared dimension ingestion
    # ------------------------------------------------------------------

    @task(task_id="ingest_bronze_products")
    def ingest_bronze_products() -> int:
        from src.config import get_spark_session
        from src.ingestion.bronze_products import ingest_bronze_products as _fn

        spark = get_spark_session()
        rows = _fn(spark)
        log.info("Bronze products: %d rows ingested", rows)
        return rows

    # ------------------------------------------------------------------
    # Delta branch
    # ------------------------------------------------------------------

    @task(task_id="ingest_bronze_delta")
    def ingest_bronze_delta() -> int:
        from src.config import get_spark_session
        from src.ingestion.bronze_ingest import ingest_bronze

        spark = get_spark_session()
        rows = ingest_bronze(spark, format="delta")
        log.info("Bronze Delta: %d rows ingested", rows)
        return rows

    @task(task_id="transform_silver_delta")
    def transform_silver_delta() -> dict:
        from src.config import get_spark_session
        from src.transformations.silver_transform import transform_silver

        spark = get_spark_session()
        stats = transform_silver(spark, format="delta")
        log.info(
            "Silver Delta: read=%d  merged=%d",
            stats["rows_read"],
            stats["rows_merged"],
        )
        return stats

    @task(task_id="enrich_silver")
    def enrich_silver() -> int:
        from src.config import get_spark_session
        from src.transformations.silver_transform import enrich_silver_with_products

        spark = get_spark_session()
        rows = enrich_silver_with_products(spark)
        log.info("Enriched silver: %d rows written", rows)
        return rows

    @task(task_id="aggregate_gold_delta")
    def aggregate_gold_delta() -> None:
        from src.config import get_spark_session
        from src.transformations.gold_aggregate import aggregate_gold

        spark = get_spark_session()
        aggregate_gold(spark, format="delta")
        log.info("Gold Delta: region + category aggregations written")

    @task(task_id="aggregate_gold_margin")
    def aggregate_gold_margin() -> None:
        from src.config import get_spark_session
        from src.transformations.gold_aggregate import aggregate_gold_margin as _fn

        spark = get_spark_session()
        _fn(spark)
        log.info("Gold Delta: margin by category written")

    # ------------------------------------------------------------------
    # Iceberg branch (runs after Delta to avoid Spark session conflicts)
    # ------------------------------------------------------------------

    @task(task_id="ingest_bronze_iceberg")
    def ingest_bronze_iceberg() -> int:
        from src.config import get_spark_session
        from src.ingestion.bronze_ingest import ingest_bronze

        spark = get_spark_session()
        rows = ingest_bronze(spark, format="iceberg")
        log.info("Bronze Iceberg: %d rows ingested", rows)
        return rows

    @task(task_id="transform_silver_iceberg")
    def transform_silver_iceberg() -> dict:
        from src.config import get_spark_session
        from src.transformations.silver_transform import transform_silver

        spark = get_spark_session()
        stats = transform_silver(spark, format="iceberg")
        log.info(
            "Silver Iceberg: read=%d  merged=%d",
            stats["rows_read"],
            stats["rows_merged"],
        )
        return stats

    @task(task_id="aggregate_gold_iceberg")
    def aggregate_gold_iceberg() -> None:
        from src.config import get_spark_session
        from src.transformations.gold_aggregate import aggregate_gold

        spark = get_spark_session()
        aggregate_gold(spark, format="iceberg")
        log.info("Gold Iceberg: region + category aggregations written")

    # ------------------------------------------------------------------
    # Wire up task dependencies
    # ------------------------------------------------------------------

    products = ingest_bronze_products()
    bronze_d = ingest_bronze_delta()
    silver_d = transform_silver_delta()
    enriched = enrich_silver()
    gold_d = aggregate_gold_delta()
    margin = aggregate_gold_margin()
    bronze_i = ingest_bronze_iceberg()
    silver_i = transform_silver_iceberg()
    gold_i = aggregate_gold_iceberg()

    # Delta branch: products + bronze in parallel → silver → enrich → gold → margin
    [products, bronze_d] >> silver_d >> enriched >> gold_d >> margin

    # Iceberg branch: starts after Delta is fully complete
    margin >> bronze_i >> silver_i >> gold_i


retail_pipeline()
