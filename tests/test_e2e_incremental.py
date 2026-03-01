"""End-to-end incremental load test.

Simulates two sequential batch loads followed by a re-run of the second batch
to verify idempotency across all three medallion layers.

Scenario
--------
Batch 1 — 4 transactions on 2024-01-15 (all valid)
Batch 2 — 3 new transactions on 2024-02-10 (all valid)

Expected behaviour
------------------
After load 1:
  bronze : 4 rows
  silver : 4 unique rows (MERGE insert)
  gold   : aggregations for 2024-01-15 only

After load 2:
  bronze : 7 rows  (4 + 3 appended)
  silver : 7 unique rows (3 new rows MERGEd in)
  gold   : aggregations for both dates; 2024-01-15 partition unchanged

After re-running load 2 (idempotency):
  bronze : 10 rows (3 more appended — append-only by design)
  silver : 7 unique rows (MERGE deduplicates; no new inserts)
  gold   : identical to after-load-2 (replaceWhere is stable)
"""

import csv
import os
from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.ingestion.bronze_ingest import ingest_bronze
from src.transformations.gold_aggregate import aggregate_gold
from src.transformations.silver_transform import transform_silver


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CSV_FIELDS = [
    "transaction_id", "event_date", "store_id", "region", "customer_id",
    "product_id", "product_name", "category", "quantity", "unit_price",
    "discount_pct", "discount_amount", "total_amount", "payment_method",
]


def _write_csv(path: str, rows: list[dict]) -> None:
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, "batch.csv")
    with open(filepath, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _row(txn_id: str, event_date: str, region: str, category: str,
         quantity: int = 2, unit_price: float = 100.0,
         total_amount: float = 180.0) -> dict:
    return {
        "transaction_id": txn_id,
        "event_date": event_date,
        "store_id": "S001",
        "region": region,
        "customer_id": "C001",
        "product_id": "P001",
        "product_name": "Widget",
        "category": category,
        "quantity": str(quantity),
        "unit_price": str(unit_price),
        "discount_pct": "10",
        "discount_amount": "20.0",
        "total_amount": str(total_amount),
        "payment_method": "Credit Card",
    }


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

class TestIncrementalLoad:
    def test_two_batch_incremental_idempotency(self, spark: SparkSession, tmp_path):
        """Full Bronze → Silver → Gold pipeline run twice; silver and gold stay correct."""

        # ── paths ──────────────────────────────────────────────────────────
        batch1_src = str(tmp_path / "source" / "batch1")
        batch2_src = str(tmp_path / "source" / "batch2")
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")
        region_path = str(tmp_path / "gold" / "region")
        category_path = str(tmp_path / "gold" / "category")
        staging_path = str(tmp_path / "staging")  # isolated — avoids live staging table

        # ── source data ────────────────────────────────────────────────────
        batch1_rows = [
            _row("TXN000001", "2024-01-15", "North", "Electronics", total_amount=500.0),
            _row("TXN000002", "2024-01-15", "North", "Electronics", total_amount=300.0),
            _row("TXN000003", "2024-01-15", "South", "Accessories", total_amount=100.0),
            _row("TXN000004", "2024-01-15", "South", "Accessories", total_amount=200.0),
        ]
        batch2_rows = [
            _row("TXN000005", "2024-02-10", "East",  "Furniture",   total_amount=700.0),
            _row("TXN000006", "2024-02-10", "East",  "Furniture",   total_amount=400.0),
            _row("TXN000007", "2024-02-10", "West",  "Stationery",  total_amount=50.0),
        ]

        _write_csv(batch1_src, batch1_rows)
        _write_csv(batch2_src, batch2_rows)

        # ══════════════════════════════════════════════════════════════════
        # LOAD 1
        # ══════════════════════════════════════════════════════════════════
        rows_b1 = ingest_bronze(spark, source_path=batch1_src, output_path=bronze_path, staging_path=staging_path)
        assert rows_b1 == 4, "bronze: load 1 should ingest 4 rows"

        stats1 = transform_silver(spark, bronze_path=bronze_path, silver_path=silver_path)
        assert stats1["rows_read"] == 4
        assert stats1["rows_merged"] == 4

        aggregate_gold(
            spark,
            silver_path=silver_path,
            region_path=region_path,
            category_path=category_path,
        )

        # bronze after load 1
        assert spark.read.format("delta").load(bronze_path).count() == 4

        # silver after load 1: 4 unique transactions
        assert spark.read.format("delta").load(silver_path).count() == 4

        # gold after load 1: only 2024-01-15 dates present
        region_df1 = spark.read.format("delta").load(region_path)
        assert region_df1.select("event_date").distinct().count() == 1
        assert region_df1.filter(col("event_date") == "2024-01-15").count() == 2  # North, South

        # spot-check Jan region revenue
        north_jan = (
            region_df1
            .filter((col("region") == "North") & (col("event_date") == "2024-01-15"))
            .first()
        )
        assert north_jan.total_revenue == pytest.approx(800.0)   # 500 + 300
        assert north_jan.total_transactions == 2

        # ══════════════════════════════════════════════════════════════════
        # LOAD 2 — new transactions on a different date
        # ══════════════════════════════════════════════════════════════════
        rows_b2 = ingest_bronze(spark, source_path=batch2_src, output_path=bronze_path, staging_path=staging_path)
        assert rows_b2 == 3, "bronze: load 2 should ingest 3 rows"

        stats2 = transform_silver(spark, bronze_path=bronze_path, silver_path=silver_path)
        assert stats2["rows_read"] == 7   # 4 from load 1 + 3 from load 2
        assert stats2["rows_merged"] == 7  # all 7 unique transaction_ids

        aggregate_gold(
            spark,
            silver_path=silver_path,
            region_path=region_path,
            category_path=category_path,
        )

        # bronze: 4 + 3 = 7 (append-only)
        assert spark.read.format("delta").load(bronze_path).count() == 7

        # silver: 7 unique rows
        assert spark.read.format("delta").load(silver_path).count() == 7

        # gold: now covers both dates
        region_df2 = spark.read.format("delta").load(region_path)
        assert region_df2.select("event_date").distinct().count() == 2

        # Jan partition must be unchanged
        north_jan_after = (
            region_df2
            .filter((col("region") == "North") & (col("event_date") == "2024-01-15"))
            .first()
        )
        assert north_jan_after.total_revenue == pytest.approx(800.0)
        assert north_jan_after.total_transactions == 2

        # Feb East/Furniture revenue
        east_feb = (
            region_df2
            .filter((col("region") == "East") & (col("event_date") == "2024-02-10"))
            .first()
        )
        assert east_feb.total_revenue == pytest.approx(1100.0)  # 700 + 400
        assert east_feb.total_transactions == 2

        # ══════════════════════════════════════════════════════════════════
        # RE-RUN LOAD 2 — idempotency check
        # ══════════════════════════════════════════════════════════════════
        ingest_bronze(spark, source_path=batch2_src, output_path=bronze_path, staging_path=staging_path)

        stats3 = transform_silver(spark, bronze_path=bronze_path, silver_path=silver_path)

        aggregate_gold(
            spark,
            silver_path=silver_path,
            region_path=region_path,
            category_path=category_path,
        )

        # bronze: 7 + 3 = 10 (batch2 appended again — expected for append-only bronze)
        assert spark.read.format("delta").load(bronze_path).count() == 10

        # silver: still 7 — MERGE deduplicates on transaction_id
        assert spark.read.format("delta").load(silver_path).count() == 7

        # gold: identical to post-load-2 (replaceWhere is stable)
        region_df3 = spark.read.format("delta").load(region_path)
        east_feb_rerun = (
            region_df3
            .filter((col("region") == "East") & (col("event_date") == "2024-02-10"))
            .first()
        )
        assert east_feb_rerun.total_revenue == pytest.approx(1100.0)
        assert east_feb_rerun.total_transactions == 2
