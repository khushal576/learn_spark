# Week 9: PySpark ETL Engineering — Part 2

> **Phase 5 → Week 9**
>
> Prerequisites: Week 8 (Reading/Writing, Incremental Patterns)

---

## What You'll Learn This Week

Week 9 builds production-grade ETL systems on top of Week 8's foundations. You go from basic reads/writes to reliable, observable, testable, and deployable pipelines — the skills that distinguish a data engineer in interviews and on the job.

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [Delta Lake](01_delta_lake.ipynb) | ACID transactions, time travel, MERGE upsert, schema evolution, OPTIMIZE, VACUUM, CDF |
| 02 | [ETL Design Patterns](02_etl_design_patterns.ipynb) | SCD Type 1 & 2, Medallion Architecture, idempotent ETL, dedup at scale |
| 03 | [Error Handling & Monitoring](03_error_handling_monitoring.ipynb) | Safe UDFs, pipeline metrics, alerting, retry with backoff, structured logging |
| 04 | [Testing PySpark](04_testing_pyspark.ipynb) | Pure transform functions, assert_df_equal, pytest fixtures, edge cases |
| 05 | [Production Deployment](05_production_deployment.ipynb) | spark-submit, Airflow DAG, cloud platforms, packaging, exit codes |

---

## Key Rules to Remember

### Delta Lake
- Delta's `_delta_log` is a JSON transaction log — every write is one atomic entry
- Time travel reads an old version by replaying the log, not storing copies of the data
- `MERGE` is the correct upsert primitive — never delete+insert manually
- `VACUUM` deletes old Parquet files; after vacuum you cannot time-travel past the retention boundary
- `OPTIMIZE` + `ZORDER BY` = fewer, larger files with co-located data = faster filter queries
- Schema enforcement is on by default — use `mergeSchema=True` to add new columns safely
- Change Data Feed gives you row-level `INSERT/UPDATE_PREIMAGE/UPDATE_POSTIMAGE/DELETE` events

### ETL Design Patterns
- SCD Type 1: latest value only — MERGE overwrites. No history preserved.
- SCD Type 2: full history — close the old row (`end_date`, `is_current=False`), insert new row
- Medallion: Bronze (raw, append-only) → Silver (clean, validated) → Gold (aggregated, business-ready)
- Idempotent ETL: running twice produces the same result — use `replaceWhere` or partition overwrite
- SCD2 requires a surrogate key (not the natural key) as primary key, plus `is_current` + `start_date` + `end_date`

### Error Handling & Monitoring
- UDFs that can fail must wrap their body in try/except and return None on error
- Accumulators count errors across executors — only read them on the driver after an action
- Always log `input_rows`, `output_rows`, `rejected_rows`, and `duration_s` per stage
- Alert when rejection rate > threshold, duration > SLA, or output row count drops >N%
- Retry with exponential backoff for transient failures — always set a `max_retries` cap

### Testing PySpark
- Pure transformation functions (no SparkSession dependency) are the easiest to test
- One `SparkSession` shared across the entire pytest session via `scope='session'` fixture
- Always test edge cases: empty DataFrame, all-null column, single row, boundary values
- Use a deterministic `generate_test_orders(n, seed=42)` — never hardcode magic numbers in tests
- `assert_df_equal` should sort both sides before comparing to avoid flaky row-order failures

### Production Deployment
- Always use `spark-submit` (not `python`/`pyspark`) in production — proper resource negotiation
- Pass `--deploy-mode cluster` on YARN/K8s so the driver runs on the cluster, not your laptop
- Use `--conf spark.sql.shuffle.partitions=200` for production, `4` for local dev
- Exit code matters: `sys.exit(0)` = success, `sys.exit(1)` = failure — schedulers rely on this
- Package Python dependencies with `--py-files` (zip/egg) or a virtualenv archive

---

## Interview Questions Covered

1. What is the Delta Lake transaction log and how does time travel work?
2. What does VACUUM do and what is the risk of running it with retention 0?
3. What is the difference between SCD Type 1 and SCD Type 2?
4. What is the Medallion Architecture and why use three layers?
5. What makes an ETL pipeline idempotent and why does it matter?
6. Why must UDFs use try/except? What happens if they raise an exception?
7. How do you count errors across Spark executors from the driver?
8. What are the three things to alert on in a production ETL pipeline?
9. Why use `scope='session'` for the SparkSession pytest fixture?
10. What is the difference between `--deploy-mode client` and `--deploy-mode cluster`?
11. How do you package Python dependencies for a Spark job?
12. What is OPTIMIZE + Z-ORDER in Delta Lake?
13. What is Change Data Feed and when would you use it?
14. How do you make a MERGE conditional on actual data changes?

---

## How to Run

```bash
docker-compose up -d
# Open http://localhost:8888
# Navigate to week9/ and run notebooks in order
```

> **Tip:** Run Week 8 notebooks first — Week 9 builds directly on those patterns.
