"""Generate data/source/products.parquet with 10 product rows matching the sales CSV data.

Run inside the spark-master container:
    docker compose exec spark-master python scripts/generate_products_parquet.py
"""

from __future__ import annotations

import os
from datetime import date

from pyspark.sql import Row

from src.config import SOURCE_PRODUCTS_PATH, get_spark_session
from src.utils.schema_utils import PRODUCTS_SCHEMA

# ---------------------------------------------------------------------------
# Product reference data — product_ids must match P001–P010 in sales CSVs.
# cost_price is always lower than the unit_price seen in transactions.
# ---------------------------------------------------------------------------
_PRODUCTS = [
    Row(
        product_id="P001",
        product_name="Laptop Pro 15",
        category="Electronics",
        supplier="TechSupply Co.",
        cost_price=850.00,
        launch_date=date(2022, 1, 15),
        is_active=True,
    ),
    Row(
        product_id="P002",
        product_name="Wireless Mouse",
        category="Accessories",
        supplier="PeripheralWorld",
        cost_price=12.50,
        launch_date=date(2021, 6, 1),
        is_active=True,
    ),
    Row(
        product_id="P003",
        product_name="USB-C Hub",
        category="Accessories",
        supplier="PeripheralWorld",
        cost_price=18.00,
        launch_date=date(2021, 9, 10),
        is_active=True,
    ),
    Row(
        product_id="P004",
        product_name="Mechanical Keyboard",
        category="Accessories",
        supplier="KeyMasters Ltd.",
        cost_price=55.00,
        launch_date=date(2020, 11, 20),
        is_active=True,
    ),
    Row(
        product_id="P005",
        product_name="4K Monitor",
        category="Electronics",
        supplier="DisplayPro Inc.",
        cost_price=220.00,
        launch_date=date(2022, 3, 5),
        is_active=True,
    ),
    Row(
        product_id="P006",
        product_name="Webcam HD",
        category="Electronics",
        supplier="TechSupply Co.",
        cost_price=35.00,
        launch_date=date(2021, 2, 14),
        is_active=True,
    ),
    Row(
        product_id="P007",
        product_name="Standing Desk",
        category="Furniture",
        supplier="OfficeFurn GmbH",
        cost_price=180.00,
        launch_date=date(2020, 7, 1),
        is_active=True,
    ),
    Row(
        product_id="P008",
        product_name="Ergonomic Chair",
        category="Furniture",
        supplier="OfficeFurn GmbH",
        cost_price=220.00,
        launch_date=date(2020, 7, 1),
        is_active=True,
    ),
    Row(
        product_id="P009",
        product_name="Notebook Pack",
        category="Stationery",
        supplier="PaperGoods B.V.",
        cost_price=4.00,
        launch_date=date(2019, 1, 1),
        is_active=True,
    ),
    Row(
        product_id="P010",
        product_name="HDMI Cable 2m",
        category="Accessories",
        supplier="CableZone",
        cost_price=5.50,
        launch_date=date(2019, 6, 15),
        is_active=False,  # discontinued — tests margin filtering
    ),
]


def main() -> None:
    """Write the products reference data to SOURCE_PRODUCTS_PATH as Parquet."""
    spark = get_spark_session(app_name="GenerateProducts")

    # Ensure the source directory exists
    os.makedirs(os.path.dirname(SOURCE_PRODUCTS_PATH), exist_ok=True)

    df = spark.createDataFrame(_PRODUCTS, schema=PRODUCTS_SCHEMA)
    df.write.mode("overwrite").parquet(SOURCE_PRODUCTS_PATH)

    count = df.count()
    print(f"Written {count} product rows to {SOURCE_PRODUCTS_PATH}")


if __name__ == "__main__":
    main()
