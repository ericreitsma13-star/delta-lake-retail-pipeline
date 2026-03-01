"""Tests for the silver transformation layer."""

from datetime import datetime, timezone

import pytest
from chispa import assert_df_equality
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, trim, when
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

from src.utils.delta_utils import merge_into_delta
from src.utils.schema_utils import BRONZE_SCHEMA


def _make_bronze_df(spark: SparkSession, rows: list[dict]):
    """Create a bronze-style DataFrame from a list of dicts."""
    return spark.createDataFrame(
        [Row(**r) for r in rows],
        schema=BRONZE_SCHEMA,
    )


def _base_row(**overrides) -> dict:
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


class TestSilverValidation:
    def test_empty_customer_id_invalid(self, spark: SparkSession):
        """Rows with empty customer_id must get _is_valid = false."""
        rows = [
            _base_row(transaction_id="TXN000001", customer_id="C001"),
            _base_row(transaction_id="TXN000002", customer_id=""),
            _base_row(transaction_id="TXN000003", customer_id=None),
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

    def test_negative_total_amount_invalid(self, spark: SparkSession):
        """Rows with total_amount <= 0 must get _is_valid = false."""
        rows = [
            _base_row(transaction_id="TXN000001", total_amount=100.0),
            _base_row(transaction_id="TXN000002", total_amount=-50.0),
            _base_row(transaction_id="TXN000003", total_amount=0.0),
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

    def test_event_date_cast_to_date_type(self, spark: SparkSession):
        """event_date must be cast to DateType in silver output."""
        rows = [_base_row(event_date="2024-01-15")]
        bronze_df = _make_bronze_df(spark, rows)

        silver_df = bronze_df.withColumn(
            "event_date", to_date(col("event_date"), "yyyy-MM-dd")
        )

        event_date_field = [
            f for f in silver_df.schema.fields if f.name == "event_date"
        ][0]
        assert isinstance(event_date_field.dataType, DateType)

    def test_deduplication_keeps_latest(self, spark: SparkSession):
        """Duplicate transaction_id should keep only the row with latest _ingested_at."""
        from pyspark.sql.functions import rank
        from pyspark.sql.window import Window

        older_ts = datetime(2024, 1, 15, 8, 0, 0)
        newer_ts = datetime(2024, 1, 15, 12, 0, 0)

        rows = [
            _base_row(
                transaction_id="TXN000001",
                total_amount=100.0,
                _ingested_at=older_ts,
            ),
            _base_row(
                transaction_id="TXN000001",
                total_amount=200.0,
                _ingested_at=newer_ts,
            ),
        ]
        bronze_df = _make_bronze_df(spark, rows)

        window_spec = Window.partitionBy("transaction_id").orderBy(
            col("_ingested_at").desc()
        )
        deduped_df = (
            bronze_df.withColumn("_rank", rank().over(window_spec))
            .filter(col("_rank") == 1)
            .drop("_rank")
        )

        assert deduped_df.count() == 1
        result = deduped_df.select("total_amount").first()
        assert result.total_amount == 200.0

    def test_merge_upsert_new_row_inserted(self, spark: SparkSession, tmp_path):
        """A new transaction_id not in the target must be inserted on MERGE."""
        delta_path = str(tmp_path / "silver_merge_test")

        rows_initial = [_base_row(transaction_id="TXN000001")]
        initial_df = _make_bronze_df(spark, rows_initial)
        silver_initial = initial_df.withColumn(
            "event_date", to_date(col("event_date"), "yyyy-MM-dd")
        ).withColumn(
            "_is_valid",
            col("customer_id").isNotNull() & (col("total_amount") > 0),
        ).withColumn("_updated_at", current_timestamp())

        merge_into_delta(
            spark=spark,
            source_df=silver_initial,
            target_path=delta_path,
            merge_keys=["transaction_id"],
        )

        rows_new = [_base_row(transaction_id="TXN000002")]
        new_df = _make_bronze_df(spark, rows_new)
        silver_new = new_df.withColumn(
            "event_date", to_date(col("event_date"), "yyyy-MM-dd")
        ).withColumn(
            "_is_valid",
            col("customer_id").isNotNull() & (col("total_amount") > 0),
        ).withColumn("_updated_at", current_timestamp())

        merge_into_delta(
            spark=spark,
            source_df=silver_new,
            target_path=delta_path,
            merge_keys=["transaction_id"],
        )

        result_df = spark.read.format("delta").load(delta_path)
        assert result_df.count() == 2
        ids = {row.transaction_id for row in result_df.select("transaction_id").collect()}
        assert "TXN000001" in ids
        assert "TXN000002" in ids

    def test_merge_upsert_existing_row_updated(self, spark: SparkSession, tmp_path):
        """An existing transaction_id must be updated (not duplicated) on MERGE."""
        delta_path = str(tmp_path / "silver_merge_update_test")

        rows_initial = [_base_row(transaction_id="TXN000001", total_amount=100.0)]
        initial_df = _make_bronze_df(spark, rows_initial)
        silver_initial = initial_df.withColumn(
            "event_date", to_date(col("event_date"), "yyyy-MM-dd")
        ).withColumn(
            "_is_valid",
            col("customer_id").isNotNull() & (col("total_amount") > 0),
        ).withColumn("_updated_at", current_timestamp())

        merge_into_delta(
            spark=spark,
            source_df=silver_initial,
            target_path=delta_path,
            merge_keys=["transaction_id"],
        )

        rows_update = [_base_row(transaction_id="TXN000001", total_amount=999.0)]
        update_df = _make_bronze_df(spark, rows_update)
        silver_update = update_df.withColumn(
            "event_date", to_date(col("event_date"), "yyyy-MM-dd")
        ).withColumn(
            "_is_valid",
            col("customer_id").isNotNull() & (col("total_amount") > 0),
        ).withColumn("_updated_at", current_timestamp())

        merge_into_delta(
            spark=spark,
            source_df=silver_update,
            target_path=delta_path,
            merge_keys=["transaction_id"],
        )

        result_df = spark.read.format("delta").load(delta_path)
        assert result_df.count() == 1
        updated_row = result_df.filter(col("transaction_id") == "TXN000001").first()
        assert updated_row.total_amount == 999.0
