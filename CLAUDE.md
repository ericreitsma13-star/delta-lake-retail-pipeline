# Project: Delta Lake Retail Sales Pipeline

## Overview
A local Delta Lake medallion pipeline that ingests retail sales transaction CSV files
and processes them through bronze, silver, and gold layers using PySpark and delta-spark
running in Docker. Demonstrates incremental batch ingestion, CDC-style upserts, and
business-level aggregations on a realistic sales dataset.

## Stack
- **Runtime**: PySpark 3.5.0 with delta-spark 3.2.0
- **Storage**: Local filesystem (`./data/` directory)
- **Containerisation**: Docker Compose (Spark master + worker)
- **Python**: 3.11
- **Testing**: pytest + chispa (DataFrame equality assertions)
- **Orchestration**: Manual script-based (`run_pipeline.py`)

## Source Dataset
Two CSV files simulating incremental loads of retail sales transactions:
- `data/source/sales_jan_mar_2024.csv` — 1,000 rows, Jan–Mar 2024
- `data/source/sales_apr_jun_2024.csv` — 1,000 rows, Apr–Jun 2024

### Source Schema (CSV)
```
transaction_id  : string   — primary key (e.g. TXN000001)
event_date      : string   — transaction date yyyy-MM-dd
store_id        : string   — store identifier (S001–S010)
region          : string   — North / South / East / West / Central
customer_id     : string   — customer identifier (may be empty — data quality issue)
product_id      : string   — product identifier (P001–P010)
product_name    : string   — human-readable product name
category        : string   — Electronics / Accessories / Furniture / Stationery
quantity        : integer  — units sold
unit_price      : double   — price per unit
discount_pct    : integer  — discount percentage (0, 5, 10, 15, 20)
discount_amount : double   — calculated discount value
total_amount    : double   — net transaction value (may be negative — data quality issue)
payment_method  : string   — Credit Card / Debit Card / Cash / Online Transfer
```

### Known Data Quality Issues (handled in Silver)
- ~2% of rows have empty `customer_id` → flag as `_is_valid = false`
- ~1% of rows have negative `total_amount` → flag as `_is_valid = false`

## Project Structure
```
project/
├── CLAUDE.md
├── README.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── data/
│   ├── source/                      # Drop CSV files here
│   │   ├── sales_jan_mar_2024.csv
│   │   └── sales_apr_jun_2024.csv
│   ├── bronze/                      # Raw Delta tables
│   │   └── sales_transactions/
│   ├── silver/                      # Cleansed Delta tables
│   │   └── sales_transactions/
│   └── gold/                        # Aggregated Delta tables
│       ├── daily_sales_by_region/
│       └── daily_sales_by_category/
├── src/
│   ├── config.py
│   ├── ingestion/
│   │   └── bronze_ingest.py
│   ├── transformations/
│   │   ├── silver_transform.py
│   │   └── gold_aggregate.py
│   └── utils/
│       ├── delta_utils.py
│       └── schema_utils.py
├── tests/
│   ├── conftest.py
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
└── run_pipeline.py
```

## Architecture: Medallion Layers

### Bronze — Raw Ingestion
- Read CSV files from `./data/source/` using explicit StructType (no inferSchema)
- Append-only writes to Delta table at `./data/bronze/sales_transactions/`
- Add metadata columns: `_ingested_at` (timestamp), `_source_file` (input filename)
- Derive `ingestion_date` (date) from `_ingested_at` for partitioning
- Partition by `ingestion_date`
- Enable `mergeSchema = true`
- No transformations — raw data preserved exactly

### Silver — Cleansed & Conformed
- Read from bronze Delta table
- Cast `event_date` string → DateType
- Validate: `_is_valid = false` where `customer_id` is null/empty OR `total_amount` <= 0
- Deduplicate by `transaction_id` keeping latest `_ingested_at`
- MERGE (upsert) into silver on `transaction_id` — never overwrite
- Partition by `event_date`
- Add `_updated_at` timestamp

### Gold — Aggregated Business Layer

#### `daily_sales_by_region`
- Group by: `event_date`, `region`
- Metrics: `total_revenue`, `total_transactions`, `total_units`, `avg_discount_pct`
- Partition by `event_date`, ZORDER by `region`

#### `daily_sales_by_category`
- Group by: `event_date`, `category`
- Metrics: `total_revenue`, `total_transactions`, `total_units`, `avg_unit_price`
- Partition by `event_date`, ZORDER by `category`

- Write gold using `replaceWhere` for partition-safe overwrites
- Run OPTIMIZE + ZORDER after each write

## Delta Table Properties (all tables)
```python
delta.autoOptimize.optimizeWrite = true
delta.autoOptimize.autoCompact = true
delta.enableChangeDataFeed = true
```

## SparkSession Config (required for Delta locally)
```python
spark.sql.extensions                          = io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog               = org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.databricks.delta.retentionDurationCheck.enabled = false
```

## Coding Conventions
- Use `get_spark_session()` from `src/config.py` — never inline
- All file paths from `src/config.py` constants — no hardcoded paths in logic files
- Use explicit StructType from `src/utils/schema_utils.py` — never inferSchema
- MERGE logic lives in `src/utils/delta_utils.py` — reuse across layers
- Every transformation function must be pure and independently testable
- Type hints and docstrings required on all public functions

## Testing Conventions
- Shared `spark` fixture from `tests/conftest.py` (local mode, Delta enabled)
- Use `chispa`: `assert_df_equality(actual, expected, ignore_row_order=True)`
- Use `tmp_path` for Delta writes in tests
- Run: `docker compose exec spark-master pytest tests/ -v --tb=short`

## Running the Pipeline
```bash
docker compose up -d
docker compose exec spark-master python run_pipeline.py
docker compose exec spark-master pytest tests/ -v
```

## Common Pitfalls to Avoid
- Never use `inferSchema=True` — always explicit StructType
- Never `.write.mode("overwrite")` on silver — always MERGE
- Never create multiple SparkSessions — use singleton from config.py
- Always set Delta SQL extensions in SparkSession config
- Always use `replaceWhere` on gold, not full table overwrite
