"""Tests for the FastAPI retail pipeline service.

Strategy:
- Use FastAPI's TestClient (synchronous) — no real HTTP server needed.
- Gold table paths are monkeypatched on src.config so route handlers read
  small Delta tables written to tmp_path instead of the real data directory.
- The shared `spark` session from conftest is reused: get_spark_session()
  returns the same session (PySpark singleton), so no second JVM starts.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
from fastapi.testclient import TestClient
from pyspark.sql import Row, SparkSession
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

import src.config as config


# ---------------------------------------------------------------------------
# Minimal schemas for test gold tables
# ---------------------------------------------------------------------------

_REGION_SCHEMA = StructType(
    [
        StructField("event_date", DateType(), nullable=False),
        StructField("region", StringType(), nullable=False),
        StructField("total_revenue", DoubleType(), nullable=True),
        StructField("total_transactions", IntegerType(), nullable=True),
        StructField("total_units", IntegerType(), nullable=True),
        StructField("avg_discount_pct", DoubleType(), nullable=True),
    ]
)

_CATEGORY_SCHEMA = StructType(
    [
        StructField("event_date", DateType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("total_revenue", DoubleType(), nullable=True),
        StructField("total_transactions", IntegerType(), nullable=True),
        StructField("total_units", IntegerType(), nullable=True),
        StructField("avg_unit_price", DoubleType(), nullable=True),
    ]
)

_MARGIN_SCHEMA = StructType(
    [
        StructField("event_date", DateType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("total_revenue", DoubleType(), nullable=True),
        StructField("total_cost", DoubleType(), nullable=True),
        StructField("gross_margin", DoubleType(), nullable=True),
        StructField("margin_pct", DoubleType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Fixture: write minimal gold Delta tables + monkeypatch config + TestClient
# ---------------------------------------------------------------------------

@pytest.fixture()
def api_client(spark: SparkSession, tmp_path, monkeypatch):
    """Provide a TestClient backed by test Delta tables in tmp_path.

    Monkeypatches src.config path constants so the route handlers read/write
    to isolated tmp_path locations instead of the real data directory.
    """
    region_path = str(tmp_path / "gold_region")
    category_path = str(tmp_path / "gold_category")
    margin_path = str(tmp_path / "gold_margin")
    staging_path = str(tmp_path / "staging")

    # Write one row to each gold table so health check returns true
    spark.createDataFrame(
        [Row(event_date=date(2024, 1, 15), region="North",
             total_revenue=1000.0, total_transactions=5, total_units=10, avg_discount_pct=8.0)],
        schema=_REGION_SCHEMA,
    ).write.format("delta").mode("overwrite").partitionBy("event_date").save(region_path)

    spark.createDataFrame(
        [Row(event_date=date(2024, 1, 15), category="Electronics",
             total_revenue=800.0, total_transactions=3, total_units=4, avg_unit_price=200.0)],
        schema=_CATEGORY_SCHEMA,
    ).write.format("delta").mode("overwrite").partitionBy("event_date").save(category_path)

    spark.createDataFrame(
        [Row(event_date=date(2024, 1, 15), category="Electronics",
             total_revenue=800.0, total_cost=400.0, gross_margin=400.0, margin_pct=50.0)],
        schema=_MARGIN_SCHEMA,
    ).write.format("delta").mode("overwrite").partitionBy("event_date").save(margin_path)

    # Monkeypatch config constants before importing the app
    monkeypatch.setattr(config, "GOLD_REGION_PATH", region_path)
    monkeypatch.setattr(config, "GOLD_CATEGORY_PATH", category_path)
    monkeypatch.setattr(config, "GOLD_MARGIN_PATH", margin_path)
    monkeypatch.setattr(config, "STAGING_TRANSACTIONS_PATH", staging_path)

    # Import app after patching so route handlers see patched values at call time
    from api.main import app

    with TestClient(app, raise_server_exceptions=True) as client:
        yield client


# ---------------------------------------------------------------------------
# Valid transaction payload
# ---------------------------------------------------------------------------

_VALID_TXN = {
    "transaction_id": "TXN999999",
    "event_date": "2024-07-01",
    "store_id": "S001",
    "region": "North",
    "customer_id": "C0001",
    "product_id": "P001",
    "product_name": "Laptop Pro 15",
    "category": "Electronics",
    "quantity": 2,
    "unit_price": 1299.99,
    "discount_pct": 10,
    "discount_amount": 259.99,
    "total_amount": 2339.99,
    "payment_method": "Credit Card",
}


# ---------------------------------------------------------------------------
# POST /ingest/transaction
# ---------------------------------------------------------------------------

class TestIngestTransaction:
    def test_valid_payload_returns_accepted(self, api_client: TestClient):
        """Valid transaction must return HTTP 200 with status=accepted."""
        resp = api_client.post("/ingest/transaction", json=_VALID_TXN)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "accepted"
        assert body["transaction_id"] == "TXN999999"
        assert "staged_at" in body

    def test_missing_field_returns_422(self, api_client: TestClient):
        """Payload missing a required field must return HTTP 422 Unprocessable Entity."""
        payload = dict(_VALID_TXN)
        del payload["transaction_id"]
        resp = api_client.post("/ingest/transaction", json=payload)
        assert resp.status_code == 422

    def test_invalid_region_returns_422(self, api_client: TestClient):
        """Payload with an unsupported region must return HTTP 422."""
        payload = {**_VALID_TXN, "region": "Atlantis"}
        resp = api_client.post("/ingest/transaction", json=payload)
        assert resp.status_code == 422

    def test_zero_quantity_returns_422(self, api_client: TestClient):
        """quantity must be > 0; zero must return HTTP 422."""
        payload = {**_VALID_TXN, "quantity": 0}
        resp = api_client.post("/ingest/transaction", json=payload)
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /sales/by-region
# ---------------------------------------------------------------------------

class TestSalesByRegion:
    def test_returns_200_and_list(self, api_client: TestClient):
        """GET /sales/by-region must return HTTP 200 with a JSON list."""
        resp = api_client.get("/sales/by-region", params={
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
        })
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_returns_matching_rows(self, api_client: TestClient):
        """Rows within the date range must be returned."""
        resp = api_client.get("/sales/by-region", params={
            "start_date": "2024-01-15",
            "end_date": "2024-01-15",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["region"] == "North"

    def test_empty_result_for_out_of_range_dates(self, api_client: TestClient):
        """Date range with no data must return an empty list (not an error)."""
        resp = api_client.get("/sales/by-region", params={
            "start_date": "2020-01-01",
            "end_date": "2020-01-31",
        })
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# GET /sales/by-category
# ---------------------------------------------------------------------------

class TestSalesByCategory:
    def test_returns_200_and_list(self, api_client: TestClient):
        """GET /sales/by-category must return HTTP 200 with a JSON list."""
        resp = api_client.get("/sales/by-category", params={
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
        })
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


# ---------------------------------------------------------------------------
# GET /sales/margin
# ---------------------------------------------------------------------------

class TestSalesMargin:
    def test_returns_200_and_list(self, api_client: TestClient):
        """GET /sales/margin must return HTTP 200 with a JSON list."""
        resp = api_client.get("/sales/margin", params={
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
        })
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_margin_fields_present(self, api_client: TestClient):
        """Margin rows must include gross_margin and margin_pct fields."""
        resp = api_client.get("/sales/margin", params={
            "start_date": "2024-01-15",
            "end_date": "2024-01-15",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert "gross_margin" in data[0]
        assert "margin_pct" in data[0]


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_returns_200(self, api_client: TestClient):
        """GET /health must return HTTP 200."""
        resp = api_client.get("/health")
        assert resp.status_code == 200

    def test_all_tables_true(self, api_client: TestClient):
        """All gold tables must report true in the health response."""
        resp = api_client.get("/health")
        body = resp.json()
        assert body["status"] == "ok"
        tables = body["tables"]
        assert tables["daily_sales_by_region"] is True
        assert tables["daily_sales_by_category"] is True
        assert tables["daily_margin_by_category"] is True
