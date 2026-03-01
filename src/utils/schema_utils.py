"""Explicit StructType schemas for every layer of the medallion pipeline.

Never use inferSchema=True — all schemas are defined here and imported by
ingestion and transformation modules.
"""

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

# ---------------------------------------------------------------------------
# Source / Bronze
# ---------------------------------------------------------------------------

SOURCE_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("event_date", StringType(), nullable=True),
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
    ]
)

BRONZE_SCHEMA = StructType(
    SOURCE_SCHEMA.fields
    + [
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("_source_file", StringType(), nullable=True),
        StructField("ingestion_date", DateType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------

# event_date is promoted from StringType to DateType; two audit columns added.
_silver_base_fields = [
    f for f in BRONZE_SCHEMA.fields if f.name != "event_date"
]

SILVER_SCHEMA = StructType(
    [StructField("event_date", DateType(), nullable=True)]
    + _silver_base_fields
    + [
        StructField("_is_valid", BooleanType(), nullable=False),
        StructField("_updated_at", TimestampType(), nullable=False),
    ]
)

# ---------------------------------------------------------------------------
# Gold — daily_sales_by_region
# ---------------------------------------------------------------------------

GOLD_REGION_SCHEMA = StructType(
    [
        StructField("event_date", DateType(), nullable=False),
        StructField("region", StringType(), nullable=False),
        StructField("total_revenue", DoubleType(), nullable=True),
        StructField("total_transactions", IntegerType(), nullable=True),
        StructField("total_units", IntegerType(), nullable=True),
        StructField("avg_discount_pct", DoubleType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Gold — daily_sales_by_category
# ---------------------------------------------------------------------------

GOLD_CATEGORY_SCHEMA = StructType(
    [
        StructField("event_date", DateType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("total_revenue", DoubleType(), nullable=True),
        StructField("total_transactions", IntegerType(), nullable=True),
        StructField("total_units", IntegerType(), nullable=True),
        StructField("avg_unit_price", DoubleType(), nullable=True),
    ]
)
