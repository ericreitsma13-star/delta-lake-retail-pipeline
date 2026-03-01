"""Tests for the bronze ingestion layer."""

import os
from datetime import date

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, to_date
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from src.utils.schema_utils import BRONZE_SCHEMA, SOURCE_SCHEMA


def _make_source_csv(tmp_path, rows: list[dict]) -> str:
    """Write a minimal CSV file and return its path."""
    import csv

    filepath = str(tmp_path / "test_source.csv")
    fieldnames = [f.name for f in SOURCE_SCHEMA.fields]
    with open(filepath, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return filepath


def _sample_rows() -> list[dict]:
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


class TestBronzeIngestion:
    def test_metadata_columns_present(self, spark: SparkSession, tmp_path):
        """Bronze DataFrame must contain _ingested_at, _source_file, and ingestion_date."""
        csv_path = _make_source_csv(tmp_path, _sample_rows())

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

        columns = bronze_df.columns
        assert "_ingested_at" in columns
        assert "_source_file" in columns
        assert "ingestion_date" in columns

    def test_ingested_at_not_null(self, spark: SparkSession, tmp_path):
        """_ingested_at must be non-null for every row."""
        csv_path = _make_source_csv(tmp_path, _sample_rows())

        raw_df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(SOURCE_SCHEMA)
            .load(csv_path)
        )
        bronze_df = raw_df.withColumn("_ingested_at", current_timestamp())

        null_count = bronze_df.filter(col("_ingested_at").isNull()).count()
        assert null_count == 0

    def test_source_file_contains_filename(self, spark: SparkSession, tmp_path):
        """_source_file should contain the source CSV filename."""
        csv_path = _make_source_csv(tmp_path, _sample_rows())

        raw_df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(SOURCE_SCHEMA)
            .load(csv_path)
        )
        bronze_df = raw_df.withColumn("_source_file", input_file_name())

        rows = bronze_df.select("_source_file").collect()
        for row in rows:
            assert "test_source.csv" in row["_source_file"]

    def test_ingestion_date_matches_ingested_at(self, spark: SparkSession, tmp_path):
        """ingestion_date must equal the date portion of _ingested_at."""
        csv_path = _make_source_csv(tmp_path, _sample_rows())

        raw_df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(SOURCE_SCHEMA)
            .load(csv_path)
        )
        from pyspark.sql.functions import to_date as spark_to_date
        bronze_df = (
            raw_df.withColumn("_ingested_at", current_timestamp())
            .withColumn("ingestion_date", spark_to_date("_ingested_at"))
        )

        mismatches = bronze_df.filter(
            col("ingestion_date") != spark_to_date(col("_ingested_at"))
        ).count()
        assert mismatches == 0

    def test_row_count_matches_source(self, spark: SparkSession, tmp_path):
        """Row count in bronze must match the number of rows in the source CSV."""
        source_rows = _sample_rows()
        csv_path = _make_source_csv(tmp_path, source_rows)

        bronze_df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(SOURCE_SCHEMA)
            .load(csv_path)
        )

        assert bronze_df.count() == len(source_rows)

    def test_delta_write_and_read(self, spark: SparkSession, tmp_path):
        """Bronze data can be written to Delta and read back with correct schema."""
        csv_path = _make_source_csv(tmp_path, _sample_rows())
        delta_path = str(tmp_path / "bronze_test")

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

        (
            bronze_df.write.format("delta")
            .mode("append")
            .partitionBy("ingestion_date")
            .save(delta_path)
        )

        read_back = spark.read.format("delta").load(delta_path)
        assert read_back.count() == len(_sample_rows())
        assert "transaction_id" in read_back.columns
        assert "_ingested_at" in read_back.columns
