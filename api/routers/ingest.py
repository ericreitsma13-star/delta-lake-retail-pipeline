"""Ingest router: accepts new transaction events and stages them to Delta."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pyspark.sql import Row, SparkSession

import src.config as config
from api.dependencies import get_spark
from api.models.schemas import IngestResponse, TransactionIn

router = APIRouter(prefix="/ingest", tags=["ingest"])


@router.post("/transaction", response_model=IngestResponse)
def ingest_transaction(
    payload: TransactionIn,
    spark: SparkSession = Depends(get_spark),
) -> IngestResponse:
    """Validate and stage a single incoming transaction to the Delta staging table.

    The staged row will be picked up by the next pipeline run via ``ingest_bronze()``.

    Args:
        payload: Validated transaction data.
        spark: Injected SparkSession.

    Returns:
        Acknowledgement with ``transaction_id`` and ``staged_at`` timestamp.

    Raises:
        HTTPException 500: if writing to the staging Delta table fails.
    """
    staged_at = datetime.now(timezone.utc).isoformat()

    row = Row(
        transaction_id=payload.transaction_id,
        event_date=payload.event_date.isoformat(),
        store_id=payload.store_id,
        region=payload.region,
        customer_id=payload.customer_id,
        product_id=payload.product_id,
        product_name=payload.product_name,
        category=payload.category,
        quantity=payload.quantity,
        unit_price=float(payload.unit_price),
        discount_pct=payload.discount_pct,
        discount_amount=float(payload.discount_amount),
        total_amount=float(payload.total_amount),
        payment_method=payload.payment_method,
    )

    try:
        df = spark.createDataFrame([row])
        (
            df.write.format("delta")
            .mode("append")
            .save(config.STAGING_TRANSACTIONS_PATH)
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to stage transaction: {exc}") from exc

    return IngestResponse(
        status="accepted",
        transaction_id=payload.transaction_id,
        staged_at=staged_at,
    )
