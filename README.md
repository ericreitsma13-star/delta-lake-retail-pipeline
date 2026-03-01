# Delta Lake + Iceberg Retail Sales Pipeline

A local **medallion architecture** pipeline built with PySpark, running in Docker. Ingests retail sales CSV files and processes them through bronze, silver, and gold layers — writing to **both Delta Lake and Apache Iceberg simultaneously** on every run. Gold tables are queryable via **Trino** for ad-hoc SQL without PySpark.

Built as a portfolio project to demonstrate modern data lakehouse patterns — designed to be extended to cloud storage (ADLS, S3) or Databricks with minimal changes.

---

## Architecture

```
CSV Files  →  Bronze (raw)  →  Silver (cleansed)  →  Gold (aggregated)
               Delta + Iceberg   Delta + Iceberg        Delta + Iceberg
               append-only       MERGE / upsert         replaceWhere / MERGE
                                                               ↓
                                                        iceberg-rest (REST catalog)
                                                               ↓
                                                         Trino :8080
                                                      (ad-hoc SQL queries)
```

Both formats run in parallel on every pipeline execution. Delta and Iceberg operate through separate catalogs; the same business logic serves both. Trino connects to the Iceberg gold tables via a REST catalog bridge (`tabulario/iceberg-rest`) that sits in front of the Hadoop-catalog warehouse — no table registration step required.

| Layer | Description | Delta pattern | Iceberg pattern |
|---|---|---|---|
| **Bronze** | Raw ingestion, metadata added, no transforms | Append + `mergeSchema` | `writeTo().create()` / `.append()` |
| **Silver** | Typed, deduplicated, validated, upserted | `MERGE INTO` on `transaction_id` | SQL `MERGE INTO` on `transaction_id` |
| **Gold** | Business aggregations, query-optimised | `replaceWhere` + ZORDER | SQL `MERGE INTO` + `rewrite_data_files` |

### Bronze
- Reads all CSVs from `data/source/` using an explicit `StructType` (no `inferSchema`)
- Adds `_ingested_at` (timestamp), `_source_file` (filename), `ingestion_date` (partition key)
- Delta: appends with `mergeSchema=true`; Iceberg: `writeTo().create()` on first run, `.append()` thereafter

### Silver
- Casts `event_date` from string to `DateType`
- Flags `_is_valid = false` where `customer_id` is null/empty or `total_amount ≤ 0`
- Deduplicates by `transaction_id`, keeping the row with the latest `_ingested_at`
- Delta: `merge_into_delta()` (DeltaTable API); Iceberg: SQL `MERGE INTO` with `UPDATE SET *` / `INSERT *`

### Gold
Two aggregated tables, one per format:

**`daily_sales_by_region`** — grouped by `event_date` + `region`
```
total_revenue | total_transactions | total_units | avg_discount_pct
```

**`daily_sales_by_category`** — grouped by `event_date` + `category`
```
total_revenue | total_transactions | total_units | avg_unit_price
```

Delta tables use `replaceWhere` + ZORDER for partition-safe overwrites. Iceberg tables use SQL `MERGE INTO` with composite keys (`event_date + region` / `event_date + category`) and `rewrite_data_files` for compaction.

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
| iceberg-spark-runtime | 1.5.2 |
| Trino | 435 |
| iceberg-rest (REST catalog bridge) | tabulario/iceberg-rest:latest |
| Java | 17 (OpenJDK) |
| Docker base image | `python:3.11-slim-bookworm` |
| Testing | pytest 7.4 + chispa |

The Iceberg JAR is downloaded from Maven at container startup via `spark.jars.packages` — no Dockerfile change is needed.

---

## Project Structure

```
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── run_pipeline.py               # 6-step dual-format orchestrator
├── trino/
│   └── etc/
│       ├── config.properties     # Trino coordinator config
│       ├── node.properties
│       ├── jvm.config
│       └── catalog/
│           └── iceberg.properties  # REST catalog → iceberg-rest:8181
├── data/
│   ├── source/                   # Input CSVs (tracked)
│   ├── bronze/                   # Raw Delta table (gitignored)
│   ├── silver/                   # Cleansed Delta table (gitignored)
│   ├── gold/                     # Aggregated Delta tables (gitignored)
│   └── iceberg/                  # Iceberg warehouse (gitignored)
├── src/
│   ├── config.py                 # SparkSession singleton + Delta/Iceberg constants
│   ├── ingestion/
│   │   └── bronze_ingest.py
│   ├── transformations/
│   │   ├── silver_transform.py
│   │   └── gold_aggregate.py
│   └── utils/
│       ├── delta_utils.py        # merge_into_delta, optimize_table, vacuum_table
│       ├── iceberg_utils.py      # iceberg_table_exists, merge_into_iceberg, optimize_iceberg_table
│       └── schema_utils.py       # Explicit StructType schemas for all layers
└── tests/
    ├── conftest.py               # Session-scoped SparkSession fixture (Delta + Iceberg)
    ├── test_bronze.py
    ├── test_silver.py
    ├── test_gold.py
    ├── test_iceberg.py           # Bronze / Silver / Gold tests for Iceberg
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

# 3. Run the full pipeline (Delta + Iceberg)
docker compose exec spark-master python run_pipeline.py

# 4. Run tests
docker compose exec spark-master pytest tests/ -v

# 5. Query Delta gold output
docker compose exec spark-master python -c "
from src.config import get_spark_session, GOLD_REGION_PATH
spark = get_spark_session()
spark.read.format('delta').load(GOLD_REGION_PATH).orderBy('event_date', 'region').show(10)
"

# 6. Query Iceberg gold output
docker compose exec spark-master python -c "
from src.config import get_spark_session
spark = get_spark_session()
spark.table('iceberg.retail.gold_daily_sales_by_region').orderBy('event_date', 'region').show(10)
"
```

Expected pipeline output:
```
--- Delta Lake ---
[1/6] Bronze ingestion (Delta) ...     Done — 2,000 rows in 5.1s
[2/6] Silver transformation (Delta) .. Done — read 2,000 rows, merged 2,000 rows in 4.2s
[3/6] Gold aggregation (Delta) ...     Done — aggregations written in 17.7s

--- Apache Iceberg ---
[4/6] Bronze ingestion (Iceberg) ...   Done — 2,000 rows in 3.8s
[5/6] Silver transformation (Iceberg). Done — read 2,000 rows, merged 2,000 rows in 5.1s
[6/6] Gold aggregation (Iceberg) ...   Done — aggregations written in 8.4s
```

The pipeline is **idempotent**: re-running appends to bronze, but the silver MERGE deduplicates on `transaction_id` and the gold writes overwrite only the affected date partitions (both formats).

---

## Ad-hoc SQL with Trino

After running the pipeline, the Iceberg gold tables are queryable via Trino at `localhost:8080`:

```bash
# Open the Trino CLI
docker compose exec trino trino

# Inside the CLI:
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.retail;

SELECT * FROM iceberg.retail.gold_daily_sales_by_region
ORDER BY event_date, region LIMIT 10;

SELECT region, SUM(total_revenue) AS revenue
FROM iceberg.retail.gold_daily_sales_by_region
GROUP BY region ORDER BY revenue DESC;

SELECT category, SUM(total_revenue) AS revenue, AVG(avg_unit_price) AS avg_price
FROM iceberg.retail.gold_daily_sales_by_category
GROUP BY category ORDER BY revenue DESC;
```

`iceberg-rest` (`tabulario/iceberg-rest`) acts as a REST catalog bridge over the Hadoop-catalog warehouse directory. Trino discovers tables written by Spark automatically — no registration step is needed. Both `iceberg-rest` and `trino` mount `./data/iceberg` at `/app/data/iceberg` so Iceberg metadata file paths resolve identically in every container.

---

## Tests

34 tests covering all three layers in both formats, plus a full end-to-end incremental load scenario.

```
tests/test_bronze.py          (6 tests)  — metadata columns, null checks, row counts, Delta write/read
tests/test_silver.py          (6 tests)  — validation flags, date casting, deduplication, MERGE insert + update
tests/test_gold.py            (6 tests)  — aggregation correctness, valid-row filtering, Delta write/read
tests/test_e2e_incremental.py (1 test)   — two-batch incremental load + idempotency across all layers
tests/test_iceberg.py         (15 tests) — Bronze write/append, Silver validation/dedup/MERGE, Gold aggregation/upsert
```

The end-to-end test proves the following guarantees with known data:

| State | Bronze | Silver | Gold |
|---|---|---|---|
| After load 1 (4 rows, Jan) | 4 | 4 | Jan partitions only |
| After load 2 (3 new rows, Feb) | 7 | 7 | Jan unchanged + Feb added |
| After re-run of load 2 | 10 | **7** (no duplicates) | **identical to above** |

Iceberg tests use a function-scoped `iceberg_db` fixture that creates a unique namespace per test (`iceberg.test_<hex>`) and tears it down afterwards, giving full isolation without a full session restart.

```bash
docker compose exec spark-master pytest tests/ -v --tb=short
# 34 passed in ~60s
```

---

## Key Design Decisions

- **`configure_spark_with_delta_pip(extra_packages=[...])`** — required when using pip-installed `delta-spark` to put Delta JARs on the classpath. The `extra_packages` argument is used to pull the Iceberg runtime JAR from Maven at the same time, so no Dockerfile change is needed.
- **Named Iceberg catalog** — `spark_catalog` is already claimed by `DeltaCatalog` and cannot be shared. Iceberg runs in a separate named catalog (`iceberg`, hadoop type) configured alongside Delta. Tables are referenced as `iceberg.retail.<name>`.
- **Explicit `StructType` everywhere** — `inferSchema=True` is never used; all schemas live in `schema_utils.py`.
- **`merge_into_delta()` / `merge_into_iceberg()` create the table on first run** — both utilities detect a missing target and fall back to a direct write, so no pre-creation step is needed.
- **Gold Delta uses `replaceWhere`; Gold Iceberg uses SQL MERGE** — `replaceWhere` is a Delta-only API. Iceberg achieves the same partition-safe update behaviour via `MERGE INTO` with composite keys on `(event_date, region)` and `(event_date, category)`.
- **Singleton SparkSession** — guarded by a module-level `_spark = None` pattern in `config.py`; tests use their own fixture-scoped session.

---

## Extending This Project

- **Cloud storage** — swap local paths in `src/config.py` for `abfss://` (ADLS) or `s3a://` (S3); change the Iceberg catalog type from `hadoop` to `glue` or `hive`
- **Databricks** — the PySpark + Delta code runs as-is; replace the SparkSession factory with `DatabricksSession.builder.getOrCreate()` and use Unity Catalog for Iceberg
- **Orchestration** — wrap each layer in an Airflow DAG or Databricks Workflow task
- **Streaming** — replace `spark.read.csv()` in bronze with `spark.readStream` for near-real-time ingestion
- **SCD Type 2** — extend the silver MERGE to track row history instead of overwriting matched rows
- **BI / dashboards** — connect any JDBC-compatible tool (DBeaver, Metabase, Superset) to Trino at `localhost:8080` using the `iceberg` catalog

---

## Built With Claude Code

This project was scaffolded end-to-end using [Claude Code](https://claude.ai/code) in agentic mode — from a `CLAUDE.md` specification through to a working, tested, and committed pipeline.
