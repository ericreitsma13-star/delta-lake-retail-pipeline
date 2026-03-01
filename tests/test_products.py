"""Tests for products bronze ingestion, silver enrichment, and gold margin."""

from datetime import date, datetime

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col
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

from src.ingestion.bronze_products import ingest_bronze_products
from src.transformations.gold_aggregate import aggregate_gold_margin
from src.transformations.silver_transform import enrich_silver_with_products
from src.utils.schema_utils import PRODUCTS_SCHEMA


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_products_parquet(spark: SparkSession, path: str, rows: list[Row]) -> None:
    """Write test product rows to a parquet directory at *path*."""
    df = spark.createDataFrame(rows, schema=PRODUCTS_SCHEMA)
    df.write.mode("overwrite").parquet(path)


_SILVER_SCHEMA = StructType(
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


def _base_product(**overrides) -> Row:
    defaults = dict(
        product_id="P001",
        product_name="Laptop Pro 15",
        category="Electronics",
        supplier="TechSupply Co.",
        cost_price=850.0,
        launch_date=date(2022, 1, 15),
        is_active=True,
    )
    defaults.update(overrides)
    return Row(**defaults)


def _base_silver(**overrides) -> dict:
    defaults = dict(
        transaction_id="TXN000001",
        event_date=date(2024, 1, 15),
        store_id="S001",
        region="North",
        customer_id="C001",
        product_id="P001",
        product_name="Laptop Pro 15",
        category="Electronics",
        quantity=2,
        unit_price=1000.0,
        discount_pct=10,
        discount_amount=200.0,
        total_amount=1800.0,
        payment_method="Credit Card",
        _ingested_at=datetime(2024, 1, 15, 10, 0, 0),
        _source_file="sales_jan_mar_2024.csv",
        ingestion_date=date(2024, 1, 15),
        _is_valid=True,
        _updated_at=datetime(2024, 1, 15, 10, 0, 0),
    )
    defaults.update(overrides)
    return defaults


# ---------------------------------------------------------------------------
# Bronze Products
# ---------------------------------------------------------------------------

class TestBronzeProducts:
    def test_row_count_matches_source(self, spark: SparkSession, tmp_path):
        """Bronze ingest must return the same count as the source parquet."""
        src = str(tmp_path / "src_products")
        out = str(tmp_path / "bronze_products")
        _write_products_parquet(
            spark, src,
            [_base_product(product_id=f"P{i:03d}") for i in range(1, 6)],
        )
        count = ingest_bronze_products(spark, source_path=src, output_path=out)
        assert count == 5

    def test_ingested_at_not_null(self, spark: SparkSession, tmp_path):
        """Every bronze product row must have a non-null _ingested_at timestamp."""
        src = str(tmp_path / "src_products")
        out = str(tmp_path / "bronze_products")
        _write_products_parquet(spark, src, [_base_product()])
        ingest_bronze_products(spark, source_path=src, output_path=out)

        df = spark.read.format("delta").load(out)
        null_count = df.filter(col("_ingested_at").isNull()).count()
        assert null_count == 0

    def test_full_overwrite_replaces_previous(self, spark: SparkSession, tmp_path):
        """Second ingest must overwrite, not append — final count equals latest source."""
        src = str(tmp_path / "src_products")
        out = str(tmp_path / "bronze_products")

        # First load: 3 products
        _write_products_parquet(
            spark, src,
            [_base_product(product_id=f"P{i:03d}") for i in range(1, 4)],
        )
        ingest_bronze_products(spark, source_path=src, output_path=out)

        # Second load: 5 products
        _write_products_parquet(
            spark, src,
            [_base_product(product_id=f"P{i:03d}") for i in range(1, 6)],
        )
        count = ingest_bronze_products(spark, source_path=src, output_path=out)

        df = spark.read.format("delta").load(out)
        assert count == 5
        assert df.count() == 5  # not 8 (would be if append)

    def test_product_columns_preserved(self, spark: SparkSession, tmp_path):
        """All source product fields must be present in the bronze Delta table."""
        src = str(tmp_path / "src_products")
        out = str(tmp_path / "bronze_products")
        _write_products_parquet(spark, src, [_base_product()])
        ingest_bronze_products(spark, source_path=src, output_path=out)

        df = spark.read.format("delta").load(out)
        expected_cols = {f.name for f in PRODUCTS_SCHEMA.fields} | {"_ingested_at"}
        assert expected_cols.issubset(set(df.columns))


# ---------------------------------------------------------------------------
# Silver Enrichment
# ---------------------------------------------------------------------------

class TestSilverEnrichment:
    def _write_silver(self, spark, path, rows):
        df = spark.createDataFrame([Row(**r) for r in rows], schema=_SILVER_SCHEMA)
        df.write.format("delta").mode("overwrite").partitionBy("event_date").save(path)

    def _write_bronze_products(self, spark, path, rows):
        df = spark.createDataFrame(rows, schema=PRODUCTS_SCHEMA)
        df.write.format("delta").mode("overwrite").save(path)

    def test_join_adds_product_columns(self, spark: SparkSession, tmp_path):
        """Enrichment join must add supplier, cost_price, and is_active columns."""
        silver_path = str(tmp_path / "silver")
        products_path = str(tmp_path / "bronze_products")
        enriched_path = str(tmp_path / "enriched")

        self._write_silver(spark, silver_path, [_base_silver(product_id="P001")])
        self._write_bronze_products(
            spark, products_path,
            [_base_product(product_id="P001", supplier="TechSupply Co.", cost_price=850.0, is_active=True)],
        )

        enrich_silver_with_products(spark, silver_path, products_path, enriched_path)

        df = spark.read.format("delta").load(enriched_path)
        row = df.first()
        assert row.supplier == "TechSupply Co."
        assert row.cost_price == pytest.approx(850.0)
        assert row.is_active is True

    def test_left_join_keeps_unmatched_transactions(self, spark: SparkSession, tmp_path):
        """Transactions with no matching product must still appear (with nulls)."""
        silver_path = str(tmp_path / "silver")
        products_path = str(tmp_path / "bronze_products")
        enriched_path = str(tmp_path / "enriched")

        # Transaction for P999 which has no product entry
        self._write_silver(spark, silver_path, [_base_silver(product_id="P999")])
        self._write_bronze_products(
            spark, products_path,
            [_base_product(product_id="P001")],
        )

        enrich_silver_with_products(spark, silver_path, products_path, enriched_path)

        df = spark.read.format("delta").load(enriched_path)
        assert df.count() == 1
        row = df.first()
        assert row.supplier is None
        assert row.cost_price is None

    def test_row_count_equals_silver(self, spark: SparkSession, tmp_path):
        """Enriched row count must equal silver row count (left join, no row loss/gain)."""
        silver_path = str(tmp_path / "silver")
        products_path = str(tmp_path / "bronze_products")
        enriched_path = str(tmp_path / "enriched")

        silver_rows = [
            _base_silver(transaction_id=f"TXN{i:06d}", product_id="P001")
            for i in range(1, 8)
        ]
        self._write_silver(spark, silver_path, silver_rows)
        self._write_bronze_products(spark, products_path, [_base_product(product_id="P001")])

        count = enrich_silver_with_products(spark, silver_path, products_path, enriched_path)
        assert count == 7


# ---------------------------------------------------------------------------
# Gold Margin
# ---------------------------------------------------------------------------

_ENRICHED_SCHEMA = StructType(
    _SILVER_SCHEMA.fields
    + [
        StructField("supplier", StringType(), nullable=True),
        StructField("cost_price", DoubleType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=True),
    ]
)
_ENRICHED_FIELDS = [f.name for f in _ENRICHED_SCHEMA.fields]


class TestGoldMargin:
    def _write_enriched(self, spark, path, rows):
        # Build tuples in exact schema field order to avoid Row(**kwargs) alphabetical-sort pitfall
        tuples = [tuple(r[name] for name in _ENRICHED_FIELDS) for r in rows]
        df = spark.createDataFrame(tuples, schema=_ENRICHED_SCHEMA)
        df.write.format("delta").mode("overwrite").partitionBy("event_date").save(path)

    def _base_enriched(self, **overrides) -> dict:
        row = _base_silver()
        row.update({"supplier": "TechSupply Co.", "cost_price": 850.0, "is_active": True})
        row.update(overrides)
        return row

    def test_total_cost_calculation(self, spark: SparkSession, tmp_path):
        """total_cost must equal sum(cost_price * quantity)."""
        enriched_path = str(tmp_path / "enriched")
        margin_path = str(tmp_path / "margin")

        self._write_enriched(spark, enriched_path, [
            self._base_enriched(transaction_id="TXN1", quantity=2, cost_price=100.0, total_amount=250.0),
            self._base_enriched(transaction_id="TXN2", quantity=3, cost_price=50.0, total_amount=180.0),
        ])

        aggregate_gold_margin(spark, enriched_path, margin_path)

        result = spark.read.format("delta").load(margin_path).first()
        # total_cost = (2 * 100) + (3 * 50) = 200 + 150 = 350
        assert result.total_cost == pytest.approx(350.0)

    def test_gross_margin_and_margin_pct(self, spark: SparkSession, tmp_path):
        """gross_margin = revenue - cost; margin_pct = (gross_margin / revenue) * 100."""
        enriched_path = str(tmp_path / "enriched")
        margin_path = str(tmp_path / "margin")

        self._write_enriched(spark, enriched_path, [
            # revenue=500, cost=200 → margin=300, pct=60
            self._base_enriched(transaction_id="TXN1", quantity=2, cost_price=100.0, total_amount=500.0),
        ])

        aggregate_gold_margin(spark, enriched_path, margin_path)

        result = spark.read.format("delta").load(margin_path).first()
        assert result.total_revenue == pytest.approx(500.0)
        assert result.total_cost == pytest.approx(200.0)
        assert result.gross_margin == pytest.approx(300.0)
        assert result.margin_pct == pytest.approx(60.0)

    def test_filters_inactive_products(self, spark: SparkSession, tmp_path):
        """Rows where is_active = false must be excluded from margin gold."""
        enriched_path = str(tmp_path / "enriched")
        margin_path = str(tmp_path / "margin")

        self._write_enriched(spark, enriched_path, [
            self._base_enriched(transaction_id="TXN1", is_active=True, total_amount=500.0),
            self._base_enriched(transaction_id="TXN2", is_active=False, total_amount=300.0),
        ])

        aggregate_gold_margin(spark, enriched_path, margin_path)

        result = spark.read.format("delta").load(margin_path).first()
        # Only TXN1 included → revenue = 500
        assert result.total_revenue == pytest.approx(500.0)

    def test_filters_invalid_transactions(self, spark: SparkSession, tmp_path):
        """Rows where _is_valid = false must be excluded from margin gold."""
        enriched_path = str(tmp_path / "enriched")
        margin_path = str(tmp_path / "margin")

        self._write_enriched(spark, enriched_path, [
            self._base_enriched(transaction_id="TXN1", _is_valid=True, total_amount=500.0),
            self._base_enriched(transaction_id="TXN2", _is_valid=False, total_amount=200.0),
        ])

        aggregate_gold_margin(spark, enriched_path, margin_path)

        result = spark.read.format("delta").load(margin_path).first()
        assert result.total_revenue == pytest.approx(500.0)

    def test_margin_delta_write(self, spark: SparkSession, tmp_path):
        """Margin gold table can be written to Delta and read back."""
        enriched_path = str(tmp_path / "enriched")
        margin_path = str(tmp_path / "margin")

        self._write_enriched(spark, enriched_path, [
            self._base_enriched(transaction_id="TXN1", category="Electronics", total_amount=800.0),
            self._base_enriched(transaction_id="TXN2", category="Accessories", total_amount=200.0,
                                 event_date=date(2024, 1, 15), cost_price=30.0),
        ])

        aggregate_gold_margin(spark, enriched_path, margin_path)

        df = spark.read.format("delta").load(margin_path)
        assert df.count() == 2
        assert set(df.columns) == {
            "event_date", "category", "total_revenue", "total_cost", "gross_margin", "margin_pct"
        }
