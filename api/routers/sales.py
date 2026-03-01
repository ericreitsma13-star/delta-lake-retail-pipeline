"""Sales router: query gold layer tables and health check."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from delta.tables import DeltaTable
from fastapi import APIRouter, Depends, Query
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import src.config as config
from api.dependencies import get_spark
from api.models.schemas import HealthResponse

router = APIRouter(tags=["sales"])


def _default_start() -> date:
    return date.today() - timedelta(days=30)


def _read_gold(
    spark: SparkSession,
    path: str,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """Read a gold Delta table and filter to the requested date range."""
    df = (
        spark.read.format("delta")
        .load(path)
        .filter(
            (col("event_date") >= start_date.isoformat())
            & (col("event_date") <= end_date.isoformat())
        )
    )
    return [
        {k: (v.isoformat() if isinstance(v, date) else v) for k, v in row.asDict().items()}
        for row in df.collect()
    ]


@router.get("/sales/by-region")
def sales_by_region(
    start_date: date = Query(default_factory=_default_start),
    end_date: date = Query(default=date.today()),
    spark: SparkSession = Depends(get_spark),
) -> list[dict[str, Any]]:
    """Return daily sales by region filtered to *start_date*–*end_date*.

    Args:
        start_date: Inclusive start date (default: 30 days ago).
        end_date: Inclusive end date (default: today).
        spark: Injected SparkSession.

    Returns:
        List of dicts with keys event_date, region, total_revenue, total_transactions,
        total_units, avg_discount_pct.
    """
    return _read_gold(spark, config.GOLD_REGION_PATH, start_date, end_date)


@router.get("/sales/by-category")
def sales_by_category(
    start_date: date = Query(default_factory=_default_start),
    end_date: date = Query(default=date.today()),
    spark: SparkSession = Depends(get_spark),
) -> list[dict[str, Any]]:
    """Return daily sales by category filtered to *start_date*–*end_date*.

    Args:
        start_date: Inclusive start date (default: 30 days ago).
        end_date: Inclusive end date (default: today).
        spark: Injected SparkSession.

    Returns:
        List of dicts with keys event_date, category, total_revenue, total_transactions,
        total_units, avg_unit_price.
    """
    return _read_gold(spark, config.GOLD_CATEGORY_PATH, start_date, end_date)


@router.get("/sales/margin")
def sales_margin(
    start_date: date = Query(default_factory=_default_start),
    end_date: date = Query(default=date.today()),
    spark: SparkSession = Depends(get_spark),
) -> list[dict[str, Any]]:
    """Return daily margin by category filtered to *start_date*–*end_date*.

    Args:
        start_date: Inclusive start date (default: 30 days ago).
        end_date: Inclusive end date (default: today).
        spark: Injected SparkSession.

    Returns:
        List of dicts with keys event_date, category, total_revenue, total_cost,
        gross_margin, margin_pct.
    """
    return _read_gold(spark, config.GOLD_MARGIN_PATH, start_date, end_date)


@router.get("/health", response_model=HealthResponse)
def health(spark: SparkSession = Depends(get_spark)) -> HealthResponse:
    """Check that all gold Delta tables exist and are readable.

    Args:
        spark: Injected SparkSession.

    Returns:
        HealthResponse with overall status and per-table availability flags.
    """
    table_paths = {
        "daily_sales_by_region": config.GOLD_REGION_PATH,
        "daily_sales_by_category": config.GOLD_CATEGORY_PATH,
        "daily_margin_by_category": config.GOLD_MARGIN_PATH,
    }

    table_status: dict[str, bool] = {}
    for name, path in table_paths.items():
        try:
            ok = DeltaTable.isDeltaTable(spark, path)
            if ok:
                # Attempt a lightweight read to confirm the table is accessible
                spark.read.format("delta").load(path).limit(1).count()
            table_status[name] = ok
        except Exception:
            table_status[name] = False

    overall = "ok" if all(table_status.values()) else "degraded"
    return HealthResponse(status=overall, tables=table_status)
