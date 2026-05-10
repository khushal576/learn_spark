# Week 10 Quiz — Spark Structured Streaming Part 1

> Answer without looking at the notebooks. Check answers afterward.

---

## Section 1: Streaming Fundamentals (8 points)

**Q1.** What is the "unbounded table" mental model in Structured Streaming? Draw a diagram showing the relationship between the input stream, the query, and the result table.

**Q2.** What is a micro-batch? How is it different from row-by-row processing? What is the latency tradeoff?

**Q3.** You set `trigger(processingTime='30 seconds')`. Your micro-batch takes 45 seconds to complete. When does the next batch start? What if it takes only 10 seconds?

**Q4.** What does `checkpointLocation` store? List the three subdirectories and what each one contains. What happens if you restart a streaming query WITHOUT a checkpoint?

---

## Section 2: Sources & Sinks (8 points)

**Q5.** What is the `rate` source? What schema does it produce? Why is it useful for learning but not production?

**Q6.** You are reading from a file source watching `/data/events/`. A new file lands every minute. Your job processes one file per trigger (`maxFilesPerTrigger=1`). After 5 minutes, 5 files have landed. Your job was down for those 5 minutes and just restarted with the same checkpoint location. What happens?

**Q7.** Explain the difference between `foreach` and `foreachBatch`. Write pseudocode for a `foreachBatch` function that writes the batch to Parquet AND sends a count metric to an external API.

**Q8.** Which sinks provide exactly-once guarantees and which don't? Why does the Kafka sink only provide at-least-once?

---

## Section 3: Stateless vs Stateful (6 points)

**Q9.** Classify each operation as stateless or stateful:
- `filter(col > 100)`
- `groupBy("user_id").count()`
- `join(static_df, "key")`
- `dropDuplicates(["event_id"])`
- `withColumn("tax", col("amount") * 0.18)`
- `explode("items")`

**Q10.** You join a streaming DataFrame with a static product catalog using `left` join. A product is added to the catalog while the streaming query is running. Does the streaming query automatically pick up the new product? Explain.

**Q11.** Your streaming job counts events per `session_id`. After 90 days of running, the executor runs out of memory. What is the root cause? What are the two possible fixes?

---

## Section 4: Watermarks & Late Data (8 points)

**Q12.** What is the difference between event time and processing time? Give a real-world example where they differ by more than 5 minutes.

**Q13.** You have `withWatermark("event_time", "15 minutes")`. The latest event time seen is 11:45. An event arrives with `event_time = 11:25`. Is it accepted or dropped? Show your calculation.

**Q14.** Explain what happens to state store memory when you use `withWatermark("ts", "10 minutes")` + `groupBy("key").count()` + `outputMode("update")`. Contrast with the same query using `outputMode("complete")`.

**Q15.** Why must `withWatermark()` appear before `groupBy()` in your PySpark code? What happens if you call them in the wrong order?

---

## Answers

### Q1
The unbounded table model:
```
Input Stream (new rows keep arriving):
  row 1  row 2  row 3  row 4  row 5 ...
  t=0    t=1    t=2    t=3    t=4

         ↓ query (same API as batch) ↓

Result Table (continuously updated):
  Shows the result of the query applied to all rows seen so far
  Each trigger: updated with new rows or recomputed aggregations

         ↓ output mode ↓

Sink:
  append → write only new result rows
  complete → rewrite entire result table
  update → write only changed rows
```

---

### Q2
A micro-batch collects all events that arrived in a time window (e.g., 30 seconds), then processes them together as a mini-batch DataFrame. Row-by-row processing would handle one event at a time as it arrives (continuous processing). Micro-batch has higher latency (at least one trigger interval) but achieves much higher throughput by amortizing Spark job startup overhead and enabling batch-style optimizations (Catalyst, vectorized execution). Typical latency: 100ms to several seconds. True row-by-row streaming latency: ~1ms (but experimental in Spark, limited API support).

---

### Q3
- **Batch takes 45 seconds**: the next batch starts immediately when the current one finishes (no waiting — the trigger interval only sets the minimum gap). So batches run back-to-back.
- **Batch takes 10 seconds**: Spark waits the remaining 20 seconds before starting the next batch (to maintain the 30-second interval).

---

### Q4
Checkpoint subdirectories:
- `offsets/`: records which data was **read** in each batch (Kafka offset, file path, etc.) — used to know where to resume after restart
- `commits/`: records which batch IDs were **successfully written** to the sink — used to detect the last committed batch
- `state/`: stores stateful aggregation state (running counts, window accumulations, dedup seen-keys) — used to restore stateful computation after restart

Without checkpoint: Spark has no record of where it left off. On restart it reprocesses all data from the beginning (or from `startingOffsets=earliest` for Kafka). This can cause duplicate output and incorrect aggregation state.

---

### Q5
The `rate` source is a built-in synthetic source that generates rows at a configurable rate. Schema: `(timestamp TIMESTAMP, value BIGINT)`. `timestamp` = when the row was generated, `value` = monotonically increasing counter (0, 1, 2, ...). Useful for learning because it requires no external dependencies (no Kafka, no files). Not for production because it produces synthetic data with no real-world meaning and no persistence (restarted job starts value from 0 again).

---

### Q6
The streaming job restarts with the same checkpoint location. Spark reads the `offsets/` directory to see which files were already processed. Since the job was down and no files were processed, Spark sees 5 unprocessed files. With `maxFilesPerTrigger=1`, it processes them one at a time — 5 consecutive micro-batches — until it catches up. No data is lost and no file is processed twice. This is the exactly-once guarantee of the file source.

---

### Q7
Differences:
- `foreach`: called once **per row**, receives a `Row` object. Good for row-level API calls. No batch optimization.
- `foreachBatch`: called once **per micro-batch**, receives a static `DataFrame`. Full batch DataFrame API available. Much more efficient.

```python
def write_batch(batch_df, batch_id):
    count = batch_df.count()
    
    # Write to Parquet (idempotent — batch_id used for dedup)
    batch_df.write.mode("append").parquet("/output/events")
    
    # Send metric to external API (best-effort, not exactly-once)
    import requests
    requests.post("https://metrics.internal/ingest",
                  json={"batch_id": batch_id, "rows": count})

stream.writeStream.foreachBatch(write_batch).start()
```

The Parquet write is exactly-once (idempotent file sink). The API call is best-effort.

---

### Q8
| Sink | Exactly-once? | Reason |
|------|--------------|--------|
| `parquet` / `delta` | Yes | Atomic file commits + checkpoint ensure each batch written once |
| `memory` | No | In-memory, no fault tolerance |
| `console` | No | Prints are not transactional |
| `kafka` | At-least-once | Kafka producer acknowledges after write; on retry a batch may be resent |
| `foreachBatch` | Depends | Exactly-once if the batch function is idempotent (e.g., Delta MERGE with batch_id) |

---

### Q9
| Operation | Stateful? |
|-----------|----------|
| `filter(col > 100)` | Stateless |
| `groupBy("user_id").count()` | **Stateful** |
| `join(static_df, "key")` | Stateless (static side loaded once) |
| `dropDuplicates(["event_id"])` | **Stateful** (tracks seen event_ids) |
| `withColumn("tax", col("amount") * 0.18)` | Stateless |
| `explode("items")` | Stateless |

---

### Q10
The static DataFrame is evaluated when the streaming query starts (or optionally re-evaluated each micro-batch, depending on the implementation). If the product catalog is a static `spark.createDataFrame(...)` or `.read.parquet(...)` call made at startup, it is cached and the new product is NOT automatically picked up. To get the latest catalog, you would need to stop the query and restart it, or use a `foreachBatch` pattern where you re-read the catalog each batch.

---

### Q11
Root cause: `groupBy("session_id").count()` without a watermark means Spark never evicts state for old session IDs. After 90 days with millions of unique sessions, the State Store holds millions of keys → OOM.

Fix option 1: Add a watermark — `withWatermark("event_time", "2 hours").groupBy("session_id").count()`. Sessions older than 2 hours are evicted from state.

Fix option 2: Use `outputMode("complete")` only if session cardinality is small and bounded. For millions of sessions, this doesn't help — it still holds all state.

---

### Q12
Event time: the timestamp embedded in the event data — when the event actually happened.
Processing time: when Spark receives and processes the event — wall clock time.

Real-world example: A food delivery app records order placement at 7:30 PM on the customer's phone (event time). The phone has poor connectivity — the event reaches the Spark streaming job at 7:55 PM (processing time). The 25-minute gap means the event is "late." Without event-time processing, this order would be counted in the wrong time window for revenue reporting.

---

### Q13
Watermark = max(event_time seen) - threshold = 11:45 - 15 min = **11:30**.

Event `event_time = 11:25` vs watermark `11:30`:
- 11:25 < 11:30 → **DROPPED** (too late, past the watermark)

The event arrived 20 minutes after the latest event — beyond the 15-minute tolerance.

---

### Q14
With watermark + update mode:
- As new data arrives for a key, the aggregation is updated and that row is emitted.
- When the watermark advances past the last event time for a key (key's max event_time < watermark), that key's state is **evicted** from the State Store.
- Memory is bounded — state size stays proportional to the watermark window, not total runtime.

With complete mode:
- The full result table is rewritten every trigger.
- State is **never evicted** — Spark must retain every key forever (even with watermark, complete mode does not evict state).
- Memory grows indefinitely. Avoid complete mode for high-cardinality groupBys in long-running jobs.

---

### Q15
`withWatermark()` annotates the DataFrame with metadata telling Spark which column is the event-time column and what the tolerance is. This metadata must be present **before** `groupBy()` so that Spark can:
1. Use event-time semantics for the aggregation
2. Track the watermark based on the correct column
3. Know when to evict state for old groups

If you call `groupBy()` first, Spark has no event-time information and creates a non-watermarked stateful aggregation. Adding `withWatermark()` after `groupBy()` does not retroactively add event-time semantics to the aggregation — the metadata propagation doesn't work in that direction. Result: you'll either get an `AnalysisException` or the watermark will be silently ignored.
