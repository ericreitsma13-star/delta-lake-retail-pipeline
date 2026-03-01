"""Orchestrator script: runs the full Bronze → Silver → Gold pipeline for both Delta and Iceberg."""

from __future__ import annotations

import time

from src.config import get_spark_session
from src.ingestion.bronze_ingest import ingest_bronze
from src.ingestion.bronze_products import ingest_bronze_products
from src.transformations.gold_aggregate import aggregate_gold, aggregate_gold_margin
from src.transformations.silver_transform import enrich_silver_with_products, transform_silver


def _elapsed(t0: float) -> str:
    return f"{time.time() - t0:.1f}s"


def main() -> None:
    spark = get_spark_session()
    print("=" * 60)
    print("Retail Pipeline — Delta + Iceberg parallel run")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Delta pass
    # ------------------------------------------------------------------
    print("\n--- Delta Lake ---")

    print("\n[1/8] Products ingestion (Delta) ...")
    t0 = time.time()
    products_rows = ingest_bronze_products(spark)
    print(f"      Done — {products_rows:,} product rows in {_elapsed(t0)}")

    print("\n[2/8] Bronze ingestion — CSV + staging (Delta) ...")
    t0 = time.time()
    bronze_rows = ingest_bronze(spark, format="delta")
    print(f"      Done — {bronze_rows:,} rows in {_elapsed(t0)}")

    print("\n[3/8] Silver transformation (Delta) ...")
    t0 = time.time()
    silver_stats = transform_silver(spark, format="delta")
    print(
        f"      Done — read {silver_stats['rows_read']:,}, "
        f"merged {silver_stats['rows_merged']:,} rows in {_elapsed(t0)}"
    )

    print("\n[4/8] Silver enrichment with products (Delta) ...")
    t0 = time.time()
    enriched_rows = enrich_silver_with_products(spark)
    print(f"      Done — {enriched_rows:,} enriched rows in {_elapsed(t0)}")

    print("\n[5/8] Gold aggregation — region + category (Delta) ...")
    t0 = time.time()
    aggregate_gold(spark, format="delta")
    print(f"      Done — region + category written in {_elapsed(t0)}")

    print("\n[6/8] Gold aggregation — margin by category (Delta) ...")
    t0 = time.time()
    aggregate_gold_margin(spark)
    print(f"      Done — margin table written in {_elapsed(t0)}")

    # ------------------------------------------------------------------
    # Iceberg pass (transactions only — products/margin are Delta-only)
    # ------------------------------------------------------------------
    print("\n--- Apache Iceberg ---")

    print("\n[7/8] Bronze ingestion (Iceberg) ...")
    t0 = time.time()
    ice_bronze_rows = ingest_bronze(spark, format="iceberg")
    print(f"      Done — {ice_bronze_rows:,} rows in {_elapsed(t0)}")

    print("\n[8/8] Silver + Gold aggregation (Iceberg) ...")
    t0 = time.time()
    ice_silver_stats = transform_silver(spark, format="iceberg")
    aggregate_gold(spark, format="iceberg")
    print(
        f"      Done — silver merged {ice_silver_stats['rows_merged']:,} rows, "
        f"gold written in {_elapsed(t0)}"
    )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print(f"  Products        — {products_rows:,} rows")
    print(f"  Delta bronze    — {bronze_rows:,} rows  |  silver merged: {silver_stats['rows_merged']:,}")
    print(f"  Enriched silver — {enriched_rows:,} rows")
    print(f"  Iceberg bronze  — {ice_bronze_rows:,} rows  |  silver merged: {ice_silver_stats['rows_merged']:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
