"""Orchestrator script: runs the full Bronze → Silver → Gold pipeline for both Delta and Iceberg."""

from __future__ import annotations

import time

from src.config import get_spark_session
from src.ingestion.bronze_ingest import ingest_bronze
from src.transformations.gold_aggregate import aggregate_gold
from src.transformations.silver_transform import transform_silver


def _elapsed(t0: float) -> str:
    return f"{time.time() - t0:.1f}s"


def main() -> None:
    spark = get_spark_session()
    print("=" * 60)
    print("Retail Pipeline — Delta + Iceberg parallel run")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Delta pass  [1/6] → [3/6]
    # ------------------------------------------------------------------
    print("\n--- Delta Lake ---")

    print("\n[1/6] Bronze ingestion (Delta) ...")
    t0 = time.time()
    bronze_rows = ingest_bronze(spark, format="delta")
    print(f"      Done — {bronze_rows:,} rows in {_elapsed(t0)}")

    print("\n[2/6] Silver transformation (Delta) ...")
    t0 = time.time()
    silver_stats = transform_silver(spark, format="delta")
    print(
        f"      Done — read {silver_stats['rows_read']:,}, "
        f"merged {silver_stats['rows_merged']:,} rows in {_elapsed(t0)}"
    )

    print("\n[3/6] Gold aggregation (Delta) ...")
    t0 = time.time()
    aggregate_gold(spark, format="delta")
    print(f"      Done — aggregations written in {_elapsed(t0)}")

    # ------------------------------------------------------------------
    # Iceberg pass  [4/6] → [6/6]
    # ------------------------------------------------------------------
    print("\n--- Apache Iceberg ---")

    print("\n[4/6] Bronze ingestion (Iceberg) ...")
    t0 = time.time()
    ice_bronze_rows = ingest_bronze(spark, format="iceberg")
    print(f"      Done — {ice_bronze_rows:,} rows in {_elapsed(t0)}")

    print("\n[5/6] Silver transformation (Iceberg) ...")
    t0 = time.time()
    ice_silver_stats = transform_silver(spark, format="iceberg")
    print(
        f"      Done — read {ice_silver_stats['rows_read']:,}, "
        f"merged {ice_silver_stats['rows_merged']:,} rows in {_elapsed(t0)}"
    )

    print("\n[6/6] Gold aggregation (Iceberg) ...")
    t0 = time.time()
    aggregate_gold(spark, format="iceberg")
    print(f"      Done — aggregations written in {_elapsed(t0)}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print(f"  Delta   — bronze rows: {bronze_rows:,}  silver merged: {silver_stats['rows_merged']:,}")
    print(f"  Iceberg — bronze rows: {ice_bronze_rows:,}  silver merged: {ice_silver_stats['rows_merged']:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
