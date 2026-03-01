# Delta Lake Retail Sales Pipeline

A local Delta Lake **medallion architecture** pipeline built with PySpark and delta-spark, running in Docker. Ingests retail sales CSV files and processes them through bronze, silver, and gold layers.

Built as a portfolio project to demonstrate modern data lakehouse patterns — designed to be extended to cloud storage (ADLS, S3) or Databricks with minimal changes.

---

## Architecture

```
CSV Files  →  Bronze (raw)  →  Silver (cleansed)  →  Gold (aggregated)
              Delta Table       Delta Table            Delta Tables
              append-only       MERGE / upsert         replaceWhere
```

| Layer | Description | Key Pattern |
|---|---|---|
| **Bronze** | Raw ingestion, metadata added, no transforms | Append-only, partitioned by `ingestion_date` |
| **Silver** | Typed, deduplicated, validated, upserted | MERGE on `transaction_id`, partitioned by `event_date` |
| **Gold** | Business aggregations, query-optimised | `replaceWhere` + OPTIMIZE + ZORDER |

### Bronze
- Reads all CSVs from `data/source/` using an explicit `StructType` (no `inferSchema`)
- Adds `_ingested_at` (timestamp), `_source_file` (filename), `ingestion_date` (partition key)
- Appends to Delta with `mergeSchema=true`; raw data is never modified

### Silver
- Casts `event_date` from string to `DateType`
- Flags `_is_valid = false` where `customer_id` is null/empty or `total_amount ≤ 0`
- Deduplicates by `transaction_id`, keeping the row with the latest `_ingested_at`
- Upserts via `MERGE INTO` — never overwrites the whole table

### Gold
Two aggregated tables, written with `replaceWhere` so re-running a single day does not touch other partitions:

**`daily_sales_by_region`** — grouped by `event_date` + `region`
```
total_revenue | total_transactions | total_units | avg_discount_pct
```

**`daily_sales_by_category`** — grouped by `event_date` + `category`
```
total_revenue | total_transactions | total_units | avg_unit_price
```

Both tables are ZORDER'd after each write for efficient predicate pushdown.

---

## Dataset

Two CSV files simulating incremental batch loads of retail sales transactions:

| File | Rows | Period |
|---|---|---|
| `sales_jan_mar_2024.csv` | 1,000 | Jan – Mar 2024 |
| `sales_apr_jun_2024.csv` | 1,000 | Apr – Jun 2024 |

**Schema:** `transaction_id`, `event_date`, `store_id`, `region`, `customer_id`, `product_id`, `product_name`, `category`, `quantity`, `unit_price`, `discount_pct`, `discount_amount`, `total_amount`, `payment_method`

**Intentional data quality issues** (handled in Silver):
- ~2% of rows have an empty `customer_id` → flagged `_is_valid = false`
- ~1% of rows have a negative `total_amount` → flagged `_is_valid = false`

---

## Stack

| Component | Version |
|---|---|
| Python | 3.11 |
| PySpark | 3.5.0 |
| delta-spark | 3.2.0 |
| Java | 17 (OpenJDK) |
| Docker base image | `python:3.11-slim-bookworm` |
| Testing | pytest 7.4 + chispa |

---

## Project Structure

```
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── run_pipeline.py               # End-to-end orchestrator
├── data/
│   ├── source/                   # Input CSVs (tracked)
│   ├── bronze/                   # Raw Delta table (gitignored)
│   ├── silver/                   # Cleansed Delta table (gitignored)
│   └── gold/                     # Aggregated Delta tables (gitignored)
├── src/
│   ├── config.py                 # SparkSession singleton + path constants
│   ├── ingestion/
│   │   └── bronze_ingest.py
│   ├── transformations/
│   │   ├── silver_transform.py
│   │   └── gold_aggregate.py
│   └── utils/
│       ├── delta_utils.py        # merge_into_delta, optimize_table, vacuum_table
│       └── schema_utils.py       # Explicit StructType schemas for all layers
└── tests/
    ├── conftest.py               # Session-scoped SparkSession fixture
    ├── test_bronze.py
    ├── test_silver.py
    ├── test_gold.py
    └── test_e2e_incremental.py   # Two-batch incremental load + idempotency
```

---

## Quick Start

**Prerequisites:** Docker and Docker Compose.

```bash
# 1. Clone
git clone https://github.com/ericreitsma13-star/delta-lake-retail-pipeline.git
cd delta-lake-retail-pipeline

# 2. Build and start containers
docker compose up -d --build

# 3. Run the full pipeline
docker compose exec spark-master python run_pipeline.py

# 4. Run tests
docker compose exec spark-master pytest tests/ -v

# 5. Query gold output
docker compose exec spark-master python -c "
from src.config import get_spark_session, GOLD_REGION_PATH
spark = get_spark_session()
spark.read.format('delta').load(GOLD_REGION_PATH).orderBy('event_date', 'region').show(10)
"
```

Expected pipeline output:
```
[1/3] Bronze ingestion ...      Done — 2,000 rows ingested in 5.1s
[2/3] Silver transformation ... Done — read 2,000 rows, merged 2,000 rows in 4.2s
[3/3] Gold aggregation ...      Done — aggregations written in 17.7s
```

The pipeline is **idempotent**: re-running appends to bronze, but the silver MERGE deduplicates on `transaction_id` and the gold `replaceWhere` overwrites only the affected date partitions.

---

## Tests

19 tests covering all three layers plus a full end-to-end incremental load scenario, using a local-mode SparkSession (no Docker required if running pytest directly with the right Python env).

```
tests/test_bronze.py          (6 tests)  — metadata columns, null checks, row counts, Delta write/read
tests/test_silver.py          (6 tests)  — validation flags, date casting, deduplication, MERGE insert + update
tests/test_gold.py            (6 tests)  — aggregation correctness, valid-row filtering, Delta write/read
tests/test_e2e_incremental.py (1 test)   — two-batch incremental load + idempotency across all layers
```

The end-to-end test proves the following guarantees with known data:

| State | Bronze | Silver | Gold |
|---|---|---|---|
| After load 1 (4 rows, Jan) | 4 | 4 | Jan partitions only |
| After load 2 (3 new rows, Feb) | 7 | 7 | Jan unchanged + Feb added |
| After re-run of load 2 | 10 | **7** (no duplicates) | **identical to above** |

```bash
docker compose exec spark-master pytest tests/ -v --tb=short
# 19 passed in ~55s
```

---

## Key Design Decisions

- **`configure_spark_with_delta_pip()`** — required when using pip-installed `delta-spark` to add Delta JARs to the Spark classpath; plain `.getOrCreate()` causes `ClassNotFoundException` at runtime.
- **Explicit `StructType` everywhere** — `inferSchema=True` is never used; all schemas live in `schema_utils.py`.
- **`merge_into_delta()` creates the table on first run** — checks `DeltaTable.isDeltaTable()` and falls back to a direct write, so no pre-creation step is needed.
- **Gold uses `replaceWhere`** — not a full table overwrite, so re-running one day's data doesn't wipe other partitions.
- **Singleton SparkSession** — guarded by a module-level `_spark = None` pattern in `config.py`; tests use their own fixture-scoped session.

---

## Extending This Project

- **Cloud storage** — swap local paths in `src/config.py` for `abfss://` (ADLS) or `s3a://` (S3)
- **Databricks** — the PySpark + Delta code runs as-is; replace the SparkSession factory with `DatabricksSession.builder.getOrCreate()`
- **Orchestration** — wrap each layer in an Airflow DAG or Databricks Workflow task
- **Streaming** — replace `spark.read.csv()` in bronze with `spark.readStream` for near-real-time ingestion
- **SCD Type 2** — extend the silver MERGE to track row history instead of overwriting matched rows

---

## Built With Claude Code

This project was scaffolded end-to-end using [Claude Code](https://claude.ai/code) in agentic mode — from a `CLAUDE.md` specification through to a working, tested, and committed pipeline.
