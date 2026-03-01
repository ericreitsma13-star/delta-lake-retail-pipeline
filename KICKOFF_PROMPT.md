# Claude Code — Kickoff Prompt
# Paste this into your Claude Code session after opening it in the project directory.
# Edit the [PLACEHOLDERS] before running.

---

## KICKOFF PROMPT (paste into Claude Code terminal)

Plan and then build a complete Delta Lake medallion pipeline with the following specification.

### Source Data
- Format: CSV flat files dropped into `./data/source/`
- Sample schema (edit this to match your actual CSV headers):
  ```
  id: integer (primary key)
  [your_dimension_1]: string
  [your_dimension_2]: string
  [your_metric_1]: double
  [your_metric_2]: double
  event_date: date (format: yyyy-MM-dd)
  ```
- Replace the sample schema above with your real column names before approving the plan.

### What to Build
Build all files defined in the project structure in CLAUDE.md:

1. **`docker-compose.yml` + `Dockerfile`**
   - Spark master + one worker, delta-spark 3.2.0, Python 3.11
   - Mount `./data/` and `./src/` into the container
   - Expose Spark UI on port 8080

2. **`requirements.txt`**
   - pyspark==3.5.0, delta-spark==3.2.0, chispa, pytest, pytest-cov

3. **`src/config.py`**
   - Singleton SparkSession factory with all Delta Lake configs set
   - Path constants for source, bronze, silver, gold directories
   - Table name constants

4. **`src/utils/delta_utils.py`**
   - `merge_into_delta(spark, source_df, target_path, merge_keys, update_columns)` — generic MERGE helper
   - `optimize_table(spark, table_path, zorder_columns)` — runs OPTIMIZE + ZORDER
   - `vacuum_table(spark, table_path, retention_hours=168)` — runs VACUUM safely

5. **`src/utils/schema_utils.py`**
   - Explicit StructType schema definitions for source CSV, bronze, silver, gold

6. **`src/ingestion/bronze_ingest.py`**
   - Read all new CSV files from `./data/source/`
   - Add `_ingested_at`, `_source_file` metadata columns
   - Derive `ingestion_date` partition column
   - Append to bronze Delta table
   - Log row count ingested

7. **`src/transformations/silver_transform.py`**
   - Read bronze Delta table
   - Cast types, drop nulls on primary key, add `_is_valid` flag
   - Deduplicate by primary key keeping latest `_ingested_at`
   - MERGE into silver Delta table using the generic merge helper
   - Log merge stats

8. **`src/transformations/gold_aggregate.py`**
   - Read silver Delta table (filter `_is_valid = true`)
   - Aggregate: group by `[your_dimension_1]` and `event_date`, compute sum/avg of metrics
   - Write gold using `replaceWhere` on `report_date` partition
   - Run OPTIMIZE + ZORDER on gold table after write

9. **`tests/conftest.py`** — shared SparkSession fixture (local mode, Delta enabled)
10. **`tests/test_bronze.py`** — unit tests for ingestion logic
11. **`tests/test_silver.py`** — unit tests for MERGE, dedup, type casting
12. **`tests/test_gold.py`** — unit tests for aggregation logic
13. **`run_pipeline.py`** — runs bronze → silver → gold in sequence with timing logs

### Instructions
- Read CLAUDE.md fully before writing any code
- Plan first — show me the full plan including any schema or design decisions before implementing
- Ask me to confirm the CSV schema before generating StructType definitions
- Use the coding and testing conventions defined in CLAUDE.md exactly
- After building, run `pytest tests/ -v` and fix any failures before finishing
- End with a summary of what was built and how to run it

---

## TIPS FOR THE SESSION

- When Claude shows the plan, check the primary key and partition columns match your data
- If Claude picks wrong metric columns, correct it in the plan review before approving
- Use `/rewind` if a layer goes wrong — it checkpoints before each change
- After the build, drop a sample CSV into `./data/source/` and run `run_pipeline.py` to verify end-to-end
- Open a second Claude Code session with `git worktree` if you want to work on tests in parallel
