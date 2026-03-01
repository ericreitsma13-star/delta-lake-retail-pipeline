"""Tests for all three medallion layers written to Apache Iceberg."""

from __future__ import annotations

import csv
from datetime import date, datetime

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, input_file_name, sum as spark_sum, to_date, trim
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.iceberg_utils import merge_into_iceberg
from src.utils.schema_utils import BRONZE_SCHEMA, SOURCE_SCHEMA


# ---------------------------------------------------------------------------
# Shared helpers (mirror test_bronze.py / test_silver.py / test_gold.py)
# ---------------------------------------------------------------------------

def _make_source_csv(tmp_path, rows: list[dict]) -> str:
    """Write a minimal source CSV and return its path."""
    filepath = str(tmp_path / "test_source.csv")
    fieldnames = [f.name for f in SOURCE_SCHEMA.fields]
    with open(filepath, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return filepath


def _sample_source_rows() -> list[dict]:
    return [
        {
            "transaction_id": "TXN000001",
            "event_date": "2024-01-15",
            "store_id": "S001",
            "region": "North",
            "customer_id": "C001",
            "product_id": "P001",
            "product_name": "Laptop",
            "category": "Electronics",
            "quantity": "2",
            "unit_price": "999.99",
            "discount_pct": "10",
            "discount_amount": "199.99",
            "total_amount": "1799.99",
            "payment_method": "Credit Card",
        },
        {
            "transaction_id": "TXN000002",
            "event_date": "2024-01-16",
            "store_id": "S002",
            "region": "South",
            "customer_id": "C002",
            "product_id": "P002",
            "product_name": "Keyboard",
            "category": "Accessories",
            "quantity": "1",
            "unit_price": "49.99",
            "discount_pct": "5",
            "discount_amount": "2.50",
            "total_amount": "47.49",
            "payment_method": "Cash",
        },
    ]


def _make_bronze_df(spark: SparkSession, rows: list[dict]):
    """Create a bronze-style DataFrame from a list of dicts."""
    return spark.createDataFrame([Row(**r) for r in rows], schema=BRONZE_SCHEMA)


def _base_bronze_row(**overrides) -> dict:
    defaults = {
        "transaction_id": "TXN000001",
        "event_date": "2024-01-15",
        "store_id": "S001",
        "region": "North",
        "customer_id": "C001",
        "product_id": "P001",
        "product_name": "Laptop",
        "category": "Electronics",
        "quantity": 2,
        "unit_price": 999.99,
        "discount_pct": 10,
        "discount_amount": 199.99,
        "total_amount": 1799.99,
        "payment_method": "Credit Card",
        "_ingested_at": datetime(2024, 1, 15, 10, 0, 0),
        "_source_file": "sales_jan_mar_2024.csv",
        "ingestion_date": datetime(2024, 1, 15).date(),
    }
    defaults.update(overrides)
    return defaults


def _make_silver_df(spark: SparkSession, rows: list[dict]):
    """Build a minimal silver-like DataFrame for gold Iceberg tests."""
    silver_schema = StructType(
        [
            StructField("transaction_id", StringType(), nullable=False),
            StructField("event_date", DateType(), nullable=True),
            StructField("store_id", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("customer_id", StringType(), nullable=True),
            StructField("product_id", StringType(), nullable=True),
            StructField("product_name", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("quantity", IntegerType(), nullable=True),
            StructField("unit_price", DoubleType(), nullable=True),
            StructField("discount_pct", IntegerType(), nullable=True),
            StructField("discount_amount", DoubleType(), nullable=True),
            StructField("total_amount", DoubleType(), nullable=True),
            StructField("payment_method", StringType(), nullable=True),
            StructField("_ingested_at", TimestampType(), nullable=False),
            StructField("_source_file", StringType(), nullable=True),
            StructField("ingestion_date", DateType(), nullable=True),
            StructField("_is_valid", BooleanType(), nullable=False),
            StructField("_updated_at", TimestampType(), nullable=False),
        ]
    )
    return spark.createDataFrame([Row(**r) for r in rows], schema=silver_schema)


def _base_silver_row(**overrides) -> dict:
    defaults = {
        "transaction_id": "TXN000001",
        "event_date": date(2024, 1, 15),
        "store_id": "S001",
        "region": "North",
        "customer_id": "C001",
        "product_id": "P001",
        "product_name": "Laptop",
        "category": "Electronics",
        "quantity": 2,
        "unit_price": 100.0,
        "discount_pct": 10,
        "discount_amount": 20.0,
        "total_amount": 180.0,
        "payment_method": "Credit Card",
        "_ingested_at": datetime(2024, 1, 15, 10, 0, 0),
        "_source_file": "sales_jan_mar_2024.csv",
        "ingestion_date": date(2024, 1, 15),
        "_is_valid": True,
        "_updated_at": datetime(2024, 1, 15, 10, 0, 0),
    }
    defaults.update(overrides)
    return defaults


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------

class TestIcebergBronze:
    def test_metadata_columns_present(self, spark: SparkSession, iceberg_db: str, tmp_path):
        """Bronze Iceberg table must contain _ingested_at, _source_file, ingestion_date."""
        csv_path = _make_source_csv(tmp_path, _sample_source_rows())
        raw_df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(SOURCE_SCHEMA)
            .load(csv_path)
        )
        bronze_df = (
            raw_df.withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", input_file_name())
            .withColumn("ingestion_date", to_date("_ingested_at"))
        )

        assert "_ingested_at" in bronze_df.columns
        assert "_source_file" in bronze_df.columns
        assert "ingestion_date" in bronze_df.columns

    def test_row_count_matches_source(self, spark: SparkSession, iceberg_db: str, tmp_path):
        """Row count written to Iceberg must match the source CSV."""
        source_rows = _sample_source_rows()
        csv_path = _make_source_csv(tmp_path, source_rows)
        raw_df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(SOURCE_SCHEMA)
            .load(csv_path)
        )
        assert raw_df.count() == len(source_rows)

    def test_iceberg_write_and_read(self, spark: SparkSession, iceberg_db: str, tmp_path):
        """Bronze data can be written to Iceberg and read back with correct schema and count."""
        csv_path = _make_source_csv(tmp_path, _sample_source_rows())
        table = f"iceberg.{iceberg_db}.bronze_sales_transactions"

        raw_df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(SOURCE_SCHEMA)
            .load(csv_path)
        )
        bronze_df = (
            raw_df.withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", input_file_name())
            .withColumn("ingestion_date", to_date("_ingested_at"))
        )

        bronze_df.writeTo(table).using("iceberg").partitionedBy("ingestion_date").create()

        result = spark.table(table)
        assert result.count() == len(_sample_source_rows())
        assert "transaction_id" in result.columns
        assert "_ingested_at" in result.columns
        assert "ingestion_date" in result.columns

    def test_iceberg_append_accumulates_rows(self, spark: SparkSession, iceberg_db: str, tmp_path):
        """A second append to an Iceberg bronze table adds rows (does not overwrite)."""
        csv_path = _make_source_csv(tmp_path, _sample_source_rows())
        table = f"iceberg.{iceberg_db}.bronze_append_test"

        raw_df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(SOURCE_SCHEMA)
            .load(csv_path)
        )
        bronze_df = (
            raw_df.withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", input_file_name())
            .withColumn("ingestion_date", to_date("_ingested_at"))
        )

        # First write
        bronze_df.writeTo(table).using("iceberg").partitionedBy("ingestion_date").create()
        # Second write (append)
        bronze_df.writeTo(table).append()

        result = spark.table(table)
        assert result.count() == len(_sample_source_rows()) * 2


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------

class TestIcebergSilver:
    def test_validation_flag_empty_customer(self, spark: SparkSession, iceberg_db: str):
        """Empty customer_id must produce _is_valid = false in the Iceberg silver table."""
        rows = [
            _base_bronze_row(transaction_id="TXN000001", customer_id="C001"),
            _base_bronze_row(transaction_id="TXN000002", customer_id=""),
            _base_bronze_row(transaction_id="TXN000003", customer_id=None),
        ]
        bronze_df = _make_bronze_df(spark, rows)

        customer_valid = col("customer_id").isNotNull() & (trim(col("customer_id")) != "")
        amount_valid = col("total_amount") > 0
        silver_df = bronze_df.withColumn("_is_valid", customer_valid & amount_valid)

        result = {
            row.transaction_id: row._is_valid
            for row in silver_df.select("transaction_id", "_is_valid").collect()
        }
        assert result["TXN000001"] is True
        assert result["TXN000002"] is False
        assert result["TXN000003"] is False

    def test_validation_flag_negative_amount(self, spark: SparkSession, iceberg_db: str):
        """Negative total_amount must produce _is_valid = false."""
        rows = [
            _base_bronze_row(transaction_id="TXN000001", total_amount=100.0),
            _base_bronze_row(transaction_id="TXN000002", total_amount=-50.0),
            _base_bronze_row(transaction_id="TXN000003", total_amount=0.0),
        ]
        bronze_df = _make_bronze_df(spark, rows)

        amount_valid = col("total_amount") > 0
        customer_valid = col("customer_id").isNotNull() & (trim(col("customer_id")) != "")
        silver_df = bronze_df.withColumn("_is_valid", customer_valid & amount_valid)

        result = {
            row.transaction_id: row._is_valid
            for row in silver_df.select("transaction_id", "_is_valid").collect()
        }
        assert result["TXN000001"] is True
        assert result["TXN000002"] is False
        assert result["TXN000003"] is False

    def test_deduplication_keeps_latest(self, spark: SparkSession, iceberg_db: str):
        """Duplicate transaction_id keeps only the row with the latest _ingested_at."""
        from pyspark.sql.functions import rank
        from pyspark.sql.window import Window

        older_ts = datetime(2024, 1, 15, 8, 0, 0)
        newer_ts = datetime(2024, 1, 15, 12, 0, 0)
        rows = [
            _base_bronze_row(transaction_id="TXN000001", total_amount=100.0, _ingested_at=older_ts),
            _base_bronze_row(transaction_id="TXN000001", total_amount=200.0, _ingested_at=newer_ts),
        ]
        bronze_df = _make_bronze_df(spark, rows)

        window_spec = Window.partitionBy("transaction_id").orderBy(col("_ingested_at").desc())
        deduped = (
            bronze_df.withColumn("_rank", rank().over(window_spec))
            .filter(col("_rank") == 1)
            .drop("_rank")
        )

        assert deduped.count() == 1
        assert deduped.first().total_amount == 200.0

    def test_merge_new_row_inserted(self, spark: SparkSession, iceberg_db: str):
        """A new transaction_id not in the Iceberg target must be inserted on MERGE."""
        table = f"iceberg.{iceberg_db}.silver_insert_test"

        row1 = _base_bronze_row(transaction_id="TXN000001")
        df1 = _make_bronze_df(spark, [row1])
        silver1 = (
            df1.withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd"))
            .withColumn("_is_valid", col("customer_id").isNotNull() & (col("total_amount") > 0))
            .withColumn("_updated_at", current_timestamp())
        )

        merge_into_iceberg(spark, silver1, table, ["transaction_id"], partition_cols=["event_date"])

        row2 = _base_bronze_row(transaction_id="TXN000002")
        df2 = _make_bronze_df(spark, [row2])
        silver2 = (
            df2.withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd"))
            .withColumn("_is_valid", col("customer_id").isNotNull() & (col("total_amount") > 0))
            .withColumn("_updated_at", current_timestamp())
        )

        merge_into_iceberg(spark, silver2, table, ["transaction_id"], partition_cols=["event_date"])

        result = spark.table(table)
        assert result.count() == 2
        ids = {r.transaction_id for r in result.select("transaction_id").collect()}
        assert "TXN000001" in ids
        assert "TXN000002" in ids

    def test_merge_existing_row_updated(self, spark: SparkSession, iceberg_db: str):
        """An existing transaction_id must be updated (not duplicated) on Iceberg MERGE."""
        table = f"iceberg.{iceberg_db}.silver_update_test"

        row_init = _base_bronze_row(transaction_id="TXN000001", total_amount=100.0)
        df_init = _make_bronze_df(spark, [row_init])
        silver_init = (
            df_init.withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd"))
            .withColumn("_is_valid", col("customer_id").isNotNull() & (col("total_amount") > 0))
            .withColumn("_updated_at", current_timestamp())
        )

        merge_into_iceberg(spark, silver_init, table, ["transaction_id"], partition_cols=["event_date"])

        row_update = _base_bronze_row(transaction_id="TXN000001", total_amount=999.0)
        df_update = _make_bronze_df(spark, [row_update])
        silver_update = (
            df_update.withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd"))
            .withColumn("_is_valid", col("customer_id").isNotNull() & (col("total_amount") > 0))
            .withColumn("_updated_at", current_timestamp())
        )

        merge_into_iceberg(spark, silver_update, table, ["transaction_id"], partition_cols=["event_date"])

        result = spark.table(table)
        assert result.count() == 1
        assert result.filter(col("transaction_id") == "TXN000001").first().total_amount == 999.0


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------

class TestIcebergGoldRegion:
    def test_region_aggregation_correct(self, spark: SparkSession, iceberg_db: str):
        """daily_sales_by_region Iceberg aggregation must sum/count correctly."""
        rows = [
            _base_silver_row(transaction_id="TXN000001", region="North", total_amount=100.0, quantity=2, discount_pct=10),
            _base_silver_row(transaction_id="TXN000002", region="North", total_amount=200.0, quantity=3, discount_pct=5),
        ]
        silver_df = _make_silver_df(spark, rows)
        valid_df = silver_df.filter(col("_is_valid") == True)  # noqa: E712

        region_df = valid_df.groupBy("event_date", "region").agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("*").cast("integer").alias("total_transactions"),
            spark_sum("quantity").cast("integer").alias("total_units"),
            avg("discount_pct").alias("avg_discount_pct"),
        )

        result = region_df.filter(col("region") == "North").first()
        assert result.total_revenue == pytest.approx(300.0)
        assert result.total_transactions == 2
        assert result.total_units == 5
        assert result.avg_discount_pct == pytest.approx(7.5)

    def test_only_valid_rows_in_region(self, spark: SparkSession, iceberg_db: str):
        """Gold Iceberg region table must exclude _is_valid = false rows."""
        rows = [
            _base_silver_row(transaction_id="TXN000001", region="North", _is_valid=True),
            _base_silver_row(transaction_id="TXN000002", region="North", _is_valid=False),
        ]
        silver_df = _make_silver_df(spark, rows)
        valid_df = silver_df.filter(col("_is_valid") == True)  # noqa: E712

        region_df = valid_df.groupBy("event_date", "region").agg(
            count("*").cast("integer").alias("total_transactions"),
        )
        assert region_df.filter(col("region") == "North").first().total_transactions == 1

    def test_region_merge_upsert_stable(self, spark: SparkSession, iceberg_db: str):
        """Running gold region MERGE twice on the same data must not duplicate rows."""
        table = f"iceberg.{iceberg_db}.gold_region_stable_test"
        rows = [
            _base_silver_row(transaction_id="TXN000001", region="North", total_amount=100.0),
            _base_silver_row(transaction_id="TXN000002", region="South", total_amount=50.0),
        ]
        silver_df = _make_silver_df(spark, rows)
        valid_df = silver_df.filter(col("_is_valid") == True)  # noqa: E712

        region_df = valid_df.groupBy("event_date", "region").agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("*").cast("integer").alias("total_transactions"),
            spark_sum("quantity").cast("integer").alias("total_units"),
            avg("discount_pct").alias("avg_discount_pct"),
        )

        merge_into_iceberg(spark, region_df, table, ["event_date", "region"], partition_cols=["event_date"])
        merge_into_iceberg(spark, region_df, table, ["event_date", "region"], partition_cols=["event_date"])

        result = spark.table(table)
        assert result.count() == 2


class TestIcebergGoldCategory:
    def test_category_aggregation_correct(self, spark: SparkSession, iceberg_db: str):
        """daily_sales_by_category Iceberg aggregation must compute correctly."""
        rows = [
            _base_silver_row(transaction_id="TXN000001", category="Electronics", total_amount=500.0, quantity=1, unit_price=500.0),
            _base_silver_row(transaction_id="TXN000002", category="Electronics", total_amount=300.0, quantity=3, unit_price=100.0),
        ]
        silver_df = _make_silver_df(spark, rows)
        valid_df = silver_df.filter(col("_is_valid") == True)  # noqa: E712

        category_df = valid_df.groupBy("event_date", "category").agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("*").cast("integer").alias("total_transactions"),
            spark_sum("quantity").cast("integer").alias("total_units"),
            avg("unit_price").alias("avg_unit_price"),
        )

        result = category_df.filter(col("category") == "Electronics").first()
        assert result.total_revenue == pytest.approx(800.0)
        assert result.total_transactions == 2
        assert result.total_units == 4
        assert result.avg_unit_price == pytest.approx(300.0)

    def test_only_valid_rows_in_category(self, spark: SparkSession, iceberg_db: str):
        """Gold Iceberg category table must exclude _is_valid = false rows."""
        rows = [
            _base_silver_row(transaction_id="TXN000001", category="Electronics", _is_valid=True),
            _base_silver_row(transaction_id="TXN000002", category="Electronics", _is_valid=False),
        ]
        silver_df = _make_silver_df(spark, rows)
        valid_df = silver_df.filter(col("_is_valid") == True)  # noqa: E712

        category_df = valid_df.groupBy("event_date", "category").agg(
            count("*").cast("integer").alias("total_transactions"),
        )
        assert category_df.filter(col("category") == "Electronics").first().total_transactions == 1

    def test_category_merge_upsert_stable(self, spark: SparkSession, iceberg_db: str):
        """Running gold category MERGE twice on the same data must not duplicate rows."""
        table = f"iceberg.{iceberg_db}.gold_category_stable_test"
        rows = [
            _base_silver_row(transaction_id="TXN000001", category="Electronics", total_amount=100.0),
            _base_silver_row(transaction_id="TXN000002", category="Accessories", total_amount=50.0),
        ]
        silver_df = _make_silver_df(spark, rows)
        valid_df = silver_df.filter(col("_is_valid") == True)  # noqa: E712

        category_df = valid_df.groupBy("event_date", "category").agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("*").cast("integer").alias("total_transactions"),
            spark_sum("quantity").cast("integer").alias("total_units"),
            avg("unit_price").alias("avg_unit_price"),
        )

        merge_into_iceberg(spark, category_df, table, ["event_date", "category"], partition_cols=["event_date"])
        merge_into_iceberg(spark, category_df, table, ["event_date", "category"], partition_cols=["event_date"])

        result = spark.table(table)
        assert result.count() == 2
