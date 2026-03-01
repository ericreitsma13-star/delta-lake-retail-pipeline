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

| Layer  | Description | Key Pattern |
|--------|-------------|-------------|
| **Bronze** | Raw ingestion, metadata added, no transforms | Append-only, schema evolution |
| **Silver** | Typed, deduplicated, validated, upserted | MERGE on `transaction_id` |
| **Gold** | Business aggregations, query-optimised | `replaceWhere` + ZORDER |

---

## Dataset

2,000 synthetic retail sales transactions across 10 stores, 5 regions, and 10 products — split into two incremental CSV loads to simulate real-world batch ingestion.

**Columns:** `transaction_id`, `event_date`, `store_id`, `region`, `customer_id`, `product_id`, `product_name`, `category`, `quantity`, `unit_price`, `discount_pct`, `discount_amount`, `total_amount`, `payment_method`

**Intentional data quality issues** (handled in Silver):
- ~2% missing `customer_id`
- ~1% negative `total_amount`

---

## Stack

- **PySpark** 3.5.0 + **delta-spark** 3.2.0
- **Docker Compose** — local Spark master + worker
- **pytest** + **chispa** — unit testing
- **Python** 3.11

---

## Project Structure

```
├── CLAUDE.md                     # Claude Code project instructions
├── data/
│   ├── source/                   # Input CSVs
│   ├── bronze/                   # Raw Delta tables
│   ├── silver/                   # Cleansed Delta tables
│   └── gold/                     # Aggregated Delta tables
├── src/
│   ├── config.py                 # SparkSession factory + path constants
│   ├── ingestion/bronze_ingest.py
│   ├── transformations/
│   │   ├── silver_transform.py
│   │   └── gold_aggregate.py
│   └── utils/
│       ├── delta_utils.py        # MERGE, OPTIMIZE, VACUUM helpers
│       └── schema_utils.py       # Explicit StructType definitions
├── tests/
│   ├── conftest.py
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
└── run_pipeline.py               # End-to-end runner
```

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/delta-lake-retail-pipeline.git
cd delta-lake-retail-pipeline

# 2. Start the local Spark cluster
docker compose up -d

# 3. Run the full pipeline
docker compose exec spark-master python run_pipeline.py

# 4. Run tests
docker compose exec spark-master pytest tests/ -v

# 5. Query the gold layer
docker compose exec spark-master python -c "
from src.config import get_spark_session
spark = get_spark_session()
spark.read.format('delta').load('./data/gold/daily_sales_by_region').show()
"
```

---

## Gold Layer Outputs

**`daily_sales_by_region`**
| event_date | region | total_revenue | total_transactions | total_units | avg_discount_pct |
|---|---|---|---|---|---|

**`daily_sales_by_category`**
| event_date | category | total_revenue | total_transactions | total_units | avg_unit_price |
|---|---|---|---|---|---|

---

## Extending This Project

- **Cloud storage**: Swap local paths in `src/config.py` for `abfss://` (ADLS) or `s3a://` (S3)
- **Databricks**: The PySpark + Delta code runs as-is on Databricks — replace the SparkSession factory with `spark = DatabricksSession.builder.getOrCreate()`
- **Orchestration**: Wrap each layer in an Airflow DAG or Databricks Workflow
- **Streaming**: Replace `spark.read.csv()` in bronze with `spark.readStream` for near-real-time ingestion
- **Apache Iceberg**: Swap `delta` format for `iceberg` in table reads/writes — schema and logic unchanged

---

## Built With Claude Code

This project was scaffolded using [Claude Code](https://claude.ai/code) in agentic mode — demonstrating how an AI coding agent can build a complete data engineering project end-to-end from a `CLAUDE.md` specification.
