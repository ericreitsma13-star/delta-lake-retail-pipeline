"""Pydantic request/response models for the retail pipeline API."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, field_validator


VALID_REGIONS = {"North", "South", "East", "West", "Central"}
VALID_PAYMENT_METHODS = {"Credit Card", "Debit Card", "Cash", "Online Transfer"}


class TransactionIn(BaseModel):
    """Incoming transaction payload for POST /ingest/transaction."""

    transaction_id: str
    event_date: date
    store_id: str
    region: str
    customer_id: str
    product_id: str
    product_name: str
    category: str
    quantity: int = Field(..., gt=0)
    unit_price: float
    discount_pct: int = Field(..., ge=0, le=100)
    discount_amount: float
    total_amount: float
    payment_method: str

    @field_validator("region")
    @classmethod
    def region_must_be_valid(cls, v: str) -> str:
        if v not in VALID_REGIONS:
            raise ValueError(f"region must be one of {sorted(VALID_REGIONS)}")
        return v


class IngestResponse(BaseModel):
    """Response returned after a successful transaction staging."""

    status: Literal["accepted"]
    transaction_id: str
    staged_at: str


class HealthResponse(BaseModel):
    """Response from GET /health."""

    status: Literal["ok", "degraded"]
    tables: dict[str, bool]
