"""Orchestrator script: runs the full Bronze → Silver → Gold pipeline."""

from __future__ import annotations

import time

from src.config import get_spark_session
from src.ingestion.bronze_ingest import ingest_bronze
from src.transformations.gold_aggregate import aggregate_gold
from src.transformations.silver_transform import transform_silver


def main() -> None:
    spark = get_spark_session()
    print("=" * 60)
    print("Delta Lake Retail Pipeline — starting")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Bronze
    # ------------------------------------------------------------------
    print("\n[1/3] Bronze ingestion ...")
    t0 = time.time()
    bronze_rows = ingest_bronze(spark)
    bronze_elapsed = time.time() - t0
    print(f"      Done — {bronze_rows:,} rows ingested in {bronze_elapsed:.1f}s")

    # ------------------------------------------------------------------
    # Silver
    # ------------------------------------------------------------------
    print("\n[2/3] Silver transformation ...")
    t0 = time.time()
    silver_stats = transform_silver(spark)
    silver_elapsed = time.time() - t0
    print(
        f"      Done — read {silver_stats['rows_read']:,} rows, "
        f"merged {silver_stats['rows_merged']:,} rows in {silver_elapsed:.1f}s"
    )

    # ------------------------------------------------------------------
    # Gold
    # ------------------------------------------------------------------
    print("\n[3/3] Gold aggregation ...")
    t0 = time.time()
    aggregate_gold(spark)
    gold_elapsed = time.time() - t0
    print(f"      Done — aggregations written in {gold_elapsed:.1f}s")

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print(f"  Bronze rows : {bronze_rows:,}")
    print(f"  Silver merged: {silver_stats['rows_merged']:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
