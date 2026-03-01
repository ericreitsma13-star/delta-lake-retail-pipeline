"""Ingest router: accepts new transaction events and stages them to Delta."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pyspark.sql import SparkSession

import src.config as config
from api.dependencies import get_spark
from api.models.schemas import IngestResponse, TransactionIn
from src.utils.schema_utils import SOURCE_SCHEMA

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

    # Build a tuple in SOURCE_SCHEMA field order to guarantee type alignment
    # (avoids Row(**kwargs) alphabetical-sort + LongType vs IntegerType mismatches)
    data = [(
        payload.transaction_id,
        payload.event_date.isoformat(),  # event_date is StringType in SOURCE_SCHEMA
        payload.store_id,
        payload.region,
        payload.customer_id,
        payload.product_id,
        payload.product_name,
        payload.category,
        int(payload.quantity),
        float(payload.unit_price),
        int(payload.discount_pct),
        float(payload.discount_amount),
        float(payload.total_amount),
        payload.payment_method,
    )]

    try:
        df = spark.createDataFrame(data, schema=SOURCE_SCHEMA)
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
