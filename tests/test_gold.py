"""Tests for the gold aggregation layer."""

from datetime import date, datetime

import pytest
from chispa import assert_df_equality
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import avg, col, count, sum as spark_sum, to_date, trim, current_timestamp
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

from src.utils.schema_utils import BRONZE_SCHEMA, GOLD_CATEGORY_SCHEMA, GOLD_REGION_SCHEMA


def _make_silver_df(spark: SparkSession, rows: list[dict]):
    """Build a minimal silver-like DataFrame for gold tests."""
    # Silver schema: bronze columns + event_date as DateType + _is_valid + _updated_at
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


class TestGoldRegion:
    def test_region_aggregation_correct(self, spark: SparkSession):
        """daily_sales_by_region must sum/count/avg correctly for known rows."""
        rows = [
            _base_silver_row(
                transaction_id="TXN000001",
                event_date=date(2024, 1, 15),
                region="North",
                total_amount=100.0,
                quantity=2,
                discount_pct=10,
            ),
            _base_silver_row(
                transaction_id="TXN000002",
                event_date=date(2024, 1, 15),
                region="North",
                total_amount=200.0,
                quantity=3,
                discount_pct=5,
            ),
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

    def test_only_valid_rows_in_region(self, spark: SparkSession):
        """Gold region table must exclude _is_valid = false rows."""
        rows = [
            _base_silver_row(transaction_id="TXN000001", region="North", _is_valid=True),
            _base_silver_row(transaction_id="TXN000002", region="North", _is_valid=False),
        ]
        silver_df = _make_silver_df(spark, rows)
        valid_df = silver_df.filter(col("_is_valid") == True)  # noqa: E712

        region_df = valid_df.groupBy("event_date", "region").agg(
            count("*").cast("integer").alias("total_transactions"),
        )

        result = region_df.filter(col("region") == "North").first()
        assert result.total_transactions == 1

    def test_region_delta_write(self, spark: SparkSession, tmp_path):
        """Gold region table can be written to Delta and read back correctly."""
        rows = [
            _base_silver_row(transaction_id="TXN000001", region="North", total_amount=100.0),
            _base_silver_row(transaction_id="TXN000002", region="South", total_amount=200.0),
        ]
        silver_df = _make_silver_df(spark, rows)
        valid_df = silver_df.filter(col("_is_valid") == True)  # noqa: E712

        region_df = valid_df.groupBy("event_date", "region").agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("*").cast("integer").alias("total_transactions"),
            spark_sum("quantity").cast("integer").alias("total_units"),
            avg("discount_pct").alias("avg_discount_pct"),
        )

        delta_path = str(tmp_path / "gold_region")
        (
            region_df.write.format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .save(delta_path)
        )

        read_back = spark.read.format("delta").load(delta_path)
        assert read_back.count() == 2


class TestGoldCategory:
    def test_category_aggregation_correct(self, spark: SparkSession):
        """daily_sales_by_category must aggregate correctly for known rows."""
        rows = [
            _base_silver_row(
                transaction_id="TXN000001",
                event_date=date(2024, 1, 15),
                category="Electronics",
                total_amount=500.0,
                quantity=1,
                unit_price=500.0,
            ),
            _base_silver_row(
                transaction_id="TXN000002",
                event_date=date(2024, 1, 15),
                category="Electronics",
                total_amount=300.0,
                quantity=3,
                unit_price=100.0,
            ),
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

    def test_only_valid_rows_in_category(self, spark: SparkSession):
        """Gold category table must exclude _is_valid = false rows."""
        rows = [
            _base_silver_row(
                transaction_id="TXN000001", category="Electronics", _is_valid=True
            ),
            _base_silver_row(
                transaction_id="TXN000002", category="Electronics", _is_valid=False
            ),
        ]
        silver_df = _make_silver_df(spark, rows)
        valid_df = silver_df.filter(col("_is_valid") == True)  # noqa: E712

        category_df = valid_df.groupBy("event_date", "category").agg(
            count("*").cast("integer").alias("total_transactions"),
        )

        result = category_df.filter(col("category") == "Electronics").first()
        assert result.total_transactions == 1

    def test_category_delta_write(self, spark: SparkSession, tmp_path):
        """Gold category table can be written to Delta and read back correctly."""
        rows = [
            _base_silver_row(
                transaction_id="TXN000001", category="Electronics", total_amount=100.0
            ),
            _base_silver_row(
                transaction_id="TXN000002", category="Accessories", total_amount=50.0
            ),
        ]
        silver_df = _make_silver_df(spark, rows)
        valid_df = silver_df.filter(col("_is_valid") == True)  # noqa: E712

        category_df = valid_df.groupBy("event_date", "category").agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("*").cast("integer").alias("total_transactions"),
            spark_sum("quantity").cast("integer").alias("total_units"),
            avg("unit_price").alias("avg_unit_price"),
        )

        delta_path = str(tmp_path / "gold_category")
        (
            category_df.write.format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .save(delta_path)
        )

        read_back = spark.read.format("delta").load(delta_path)
        assert read_back.count() == 2
