# Week 8: PySpark ETL Engineering — Part 1

> **Phase 5 → Week 8**
>
> Prerequisites: Phase 3 (DataFrames, Spark SQL), Phase 4 (Performance)

---

## What You'll Learn This Week

Phase 5 shifts from "how Spark runs fast" to "how to build production data pipelines." Week 8 covers the fundamentals of reading messy data safely, writing in the right formats, managing schemas, enforcing data quality, and building incremental loads.

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [Reading Data Sources](01_reading_data_sources.ipynb) | CSV/JSON/Parquet/JDBC options, explicit schemas, bad record handling, partition discovery |
| 02 | [Writing Formats](02_writing_formats.ipynb) | Write modes, file count control, compression, partitionBy, format comparison |
| 03 | [Schema Management](03_schema_management.ipynb) | StructType, DDL strings, type casting, schema validation, nested schemas, flattening |
| 04 | [Data Quality](04_data_quality.ipynb) | Data profiling, validation rules, dead letter pattern, deduplication strategies |
| 05 | [Incremental Patterns](05_incremental_patterns.ipynb) | Watermark-based loads, date partition loads, upsert (MERGE), late-arriving data |

---

## Key Rules to Remember

### Reading
- Always use explicit `StructType` — never `inferSchema=True` in production
- Use `PERMISSIVE` mode + `_corrupt_record` to capture bad rows (never silently drop)
- Parquet: schema embedded in file, no parsing needed, columnar = column pruning
- Partition discovery: `year=2024/month=01/` directories auto-become columns
- JDBC: always use `partitionColumn` + `numPartitions` for parallel reads

### Writing
- `overwrite`: destructive full replacement. `append`: non-destructive (no dedup). `error`: default safe mode
- One file per partition — use `coalesce(N)` before write to reduce files (no shuffle)
- `partitionBy()`: low-cardinality columns only (date, status, region — never customer_id)
- Parquet + Snappy = default. Delta = mutable tables. Avro = Kafka/streaming

### Schema Management
- `nullable=False` is a HINT — Spark does not enforce it at runtime
- Type cast mismatches → null (not exception) in PERMISSIVE mode
- `mergeSchema=True` on Parquet/Delta to handle evolved schemas across files
- Store schemas as JSON (`.schema.json()`) for versioning

### Data Quality
- Profile first: null counts, distinct counts, range stats — in a single `.agg()` pass
- Dead letter pattern: invalid records → separate table with `_error_reasons` column
- `dropDuplicates()` is non-deterministic — prefer `row_number()` for "keep latest"
- Row-level rules as boolean columns → single pass to flag all violations

### Incremental Patterns
- Watermark = last processed timestamp/ID, stored in durable state (DB, Delta)
- Dual-write risk: always update watermark AFTER writing, atomically if possible
- Late data: re-process overlap window (last N days) to catch late arrivals
- Upsert = MERGE: requires Delta Lake for atomic correctness

---

## Interview Questions Covered

1. Why is `inferSchema=True` dangerous in production?
2. What are the three CSV bad-record modes and when do you use each?
3. How many output files does Spark create and how do you control it?
4. What is the difference between `partitionBy()` and `bucketBy()`?
5. Which file format should you use in production and why?
6. What is `nullable=False` and does Spark enforce it?
7. How do you handle a schema where upstream sometimes adds new columns?
8. What is the dead letter pattern?
9. How do you deduplicate while keeping the latest version of each record?
10. What is a watermark in batch ETL?
11. What is the dual-write problem?

---

## How to Run

```bash
docker-compose up -d
# Open http://localhost:8888
# Navigate to week8/ and run notebooks in order
```
