# Week 10: Spark Structured Streaming — Part 1

> **Phase 6 → Week 10**
>
> Prerequisites: Week 8-9 (DataFrames, ETL patterns, Delta Lake)

---

## What You'll Learn This Week

Structured Streaming is Spark's high-level API for processing live data streams using the same DataFrame API you already know. Week 10 covers the fundamentals: how streaming works, what sources and sinks are available, how to write stateless transformations, how stateful aggregations work, and how to handle late-arriving data with watermarks.

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [Structured Streaming Basics](01_structured_streaming_basics.ipynb) | Micro-batch model, triggers, output modes, checkpointing |
| 02 | [Sources & Sinks](02_sources_and_sinks.ipynb) | rate/file/socket sources, console/memory/file/foreachBatch sinks |
| 03 | [Stateless Transformations](03_stateless_transformations.ipynb) | filter/select/UDFs/explode/stream-static joins in streaming |
| 04 | [Stateful Aggregations](04_stateful_aggregations.ipynb) | groupBy().agg(), deduplication, State Store, memory safety |
| 05 | [Watermarking & Late Data](05_watermarking.ipynb) | Event time vs processing time, watermark mechanics, output modes |

---

## Key Rules to Remember

### Streaming Model
- Structured Streaming = unbounded input table + query + continuously updated result table
- Micro-batch (default) processes data in mini-batches triggered on an interval — not true row-by-row
- Every production query needs a `checkpointLocation` — without it, a restart reprocesses everything
- `query.awaitTermination()` blocks the driver; `query.stop()` terminates the query

### Triggers
- Default (`processingTime='0 seconds'`): start next batch immediately after previous finishes
- `processingTime='30 seconds'`: process every 30 seconds, skip if no data
- `once=True`: process all available data in one batch then stop (batch-like)
- `availableNow=True` (Spark 3.3+): like `once` but multi-batch (better for large backlogs)

### Output Modes
- `append`: write only new rows — works for non-aggregated queries and watermarked aggregations
- `complete`: rewrite entire result every trigger — required for aggregations without watermark (growing memory risk)
- `update`: write only changed rows — requires watermark for memory safety; most efficient for production

### Sources
- `rate`: built-in test source, generates `(timestamp, value)` rows at N rows/second
- File source: watches a directory — processes only new files (checkpoint tracks what's done)
- Always supply explicit `schema()` for file source — never `inferSchema` in streaming
- `maxFilesPerTrigger`: limits files processed per batch (prevents overload on large backlogs)

### Sinks
- `memory` + `console`: development/testing only — not fault tolerant
- `parquet`/`delta` + `checkpointLocation`: production-grade exactly-once
- `foreachBatch(fn)`: most flexible — full batch DataFrame API available in `fn`
- In `foreachBatch`, cache the batch if writing to multiple destinations, then unpersist

### Stateful Operations
- `groupBy().agg()`, `dropDuplicates()`, stream-stream joins all require state
- State is stored in the State Store (in-memory + HDFS checkpoint by default)
- Without watermark: state grows **forever** — OOM in long-running jobs
- RocksDB state store (Spark 3.2+): better for large state, disk-spillable

### Watermarks
- `withWatermark("event_time", "10 minutes")`: accept events up to 10 min late
- Watermark = `max(event_time seen) - threshold`
- Events older than watermark are **dropped** (late data tolerance)
- Watermark enables state eviction → bounded memory
- `withWatermark()` must appear **before** `groupBy()` in the chain
- Event time (in data) is almost always better than processing time (wall clock)

---

## Interview Questions Covered

1. What is Structured Streaming and how is it different from RDD-based DStreams?
2. What is a trigger and what are the four trigger types?
3. What are the three output modes and when do you use each?
4. What is a checkpoint and what does it store?
5. Which sinks guarantee exactly-once delivery?
6. What is the difference between `foreach` and `foreachBatch`?
7. What is the State Store and what happens without a watermark?
8. Can you use UDFs in Structured Streaming?
9. What is event time vs processing time?
10. How does a watermark work and what happens to late events?
11. What is the difference between `append` and `update` mode for watermarked aggregations?

---

## How to Run

```bash
docker-compose up -d
# Open http://localhost:8888
# Navigate to week10/ and run notebooks in order
```

> **Note:** Streaming notebooks run queries for a fixed time (`time.sleep(N)`) then stop. Outputs appear in the notebook's memory sink and can be queried with `spark.sql("SELECT ...")`.
