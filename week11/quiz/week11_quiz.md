# Week 11 Quiz — Spark Structured Streaming Part 2

> Answer without looking at the notebooks. Check answers afterward.

---

## Section 1: Window Operations (8 points)

**Q1.** You have a stream of sales events. Write a PySpark query that computes total revenue per category in **5-minute tumbling windows**. Include watermark, groupBy, and select the window start/end columns. Use `update` output mode.

**Q2.** You set up a sliding window: `F.window("ts", "30 minutes", "10 minutes")`. An event arrives at 10:15. How many windows does it appear in? List the start/end time of each window it belongs to.

**Q3.** What is a session window? Give a real-world use case where tumbling and sliding windows would fail but session windows are the right choice.

**Q4.** With a 10-minute tumbling window and a 5-minute watermark, when does Spark emit (finalize) the 10:00–10:10 window in `append` mode? In `update` mode?

---

## Section 2: Kafka Integration (8 points)

**Q5.** Write the complete PySpark code to: (1) read from Kafka topic `"transactions"` at `kafka:9092`, (2) parse the JSON value with schema `{txn_id: string, amount: double, currency: string}`, (3) filter amounts > 1000, (4) write to Parquet at `/output/high_value` with checkpointing.

**Q6.** What is `maxOffsetsPerTrigger` and when must you set it? What happens if you don't set it and the job restarts after being down for 6 hours with a high-volume topic?

**Q7.** You want to write enriched Kafka messages to a different Kafka topic. What two columns must your output DataFrame have? Write the `withColumn` code to serialize all columns as a JSON value.

**Q8.** Your Kafka topic has 12 partitions. How many Spark tasks run per micro-batch when reading it? You scale Kafka to 24 partitions while the job is running. What happens to parallelism?

---

## Section 3: Exactly-Once Semantics (6 points)

**Q9.** Rank these scenarios from weakest to strongest delivery guarantee. Explain each:
- Streaming job with no checkpoint, file sink
- Streaming job with checkpoint, Kafka sink
- Streaming job with checkpoint, Delta sink
- Streaming job with checkpoint, `foreachBatch` with idempotent JDBC upsert

**Q10.** Your streaming job processes batch 7 and writes 1000 rows to Parquet. Before it can commit to the checkpoint, the executor crashes. On restart, what happens? How many rows end up in Parquet?

**Q11.** Explain what makes a write operation "idempotent." Give three examples of idempotent write operations in different sink types.

---

## Section 4: Streaming Joins (6 points)

**Q12.** You join a stream of `user_events` with a static `user_profiles` table. The profiles table is updated daily with new users. How often do new users appear in the join result? What must you do to pick up new profile rows?

**Q13.** Write the stream-stream join condition for matching orders and payments within 30 minutes: orders stream (`order_id`, `order_time`) joined with payments stream (`order_id`, `payment_time`). Include watermarks and the time range constraint.

**Q14.** What is the state size of a stream-stream inner join given: 500 events/sec on each side, 1-hour watermark? How does this affect your memory sizing for Spark executors?

---

## Section 5: Production Streaming (2 points)

**Q15.** List five things that should be in every production streaming job (beyond the actual business logic).

---

## Answers

### Q1
```python
from pyspark.sql import functions as F

revenue = stream \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(
        F.window("event_time", "5 minutes"),
        "category"
    ).agg(
        F.round(F.sum("amount"), 2).alias("total_revenue"),
        F.count("*").alias("order_count")
    ) \
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "category", "total_revenue", "order_count"
    )

query = revenue.writeStream \
    .outputMode("update") \
    .format("memory").queryName("revenue_windows") \
    .trigger(processingTime="1 minute") \
    .start()
```

---

### Q2
Window size = 30 min, slide = 10 min → each event appears in `30/10 = 3` windows.

An event at 10:15 appears in:
1. `10:00–10:30` (starts at 10:00, 10:15 is within it)
2. `10:10–10:40` (starts at 10:10, 10:15 is within it)
3. `10:20–10:50` — NO. 10:15 < 10:20, so this window hasn't started yet.

Actually: windows are aligned to epoch. Windows that include 10:15:
- `09:50–10:20` (started 9:50)
- `10:00–10:30` (started 10:00)
- `10:10–10:40` (started 10:10)

So 3 windows total.

---

### Q3
A session window groups events that occur within a configurable gap of each other. If no events arrive within the gap, the current session closes and a new one starts.

Use case: **user web session tracking**. A user makes requests at 10:01, 10:03, 10:05 (active session), then is idle until 10:40, then clicks at 10:40, 10:42 (new session).

Why tumbling/sliding fails:
- Tumbling 30-min window: the user's sessions don't align to fixed time boundaries
- Sliding window: session length varies; a 5-click session and a 50-click session have different durations
- Session window: adapts to actual user behavior by closing on inactivity

---

### Q4
- **`append` mode**: the 10:00–10:10 window is emitted when the watermark advances past 10:10. With a 5-minute watermark, this happens when the latest event time reaches 10:15. So results appear only after events at 10:15+ arrive.
- **`update` mode**: partial results are emitted every trigger as data arrives. The window appears immediately when it gets its first event, and is updated with each new event. State is evicted (not emitted again) when watermark passes 10:10.

---

### Q5
```python
from pyspark.sql.types import *
from pyspark.sql import functions as F

txn_schema = StructType([
    StructField("txn_id",   StringType()),
    StructField("amount",   DoubleType()),
    StructField("currency", StringType()),
])

raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw \
    .withColumn("data", F.from_json(F.col("value").cast("string"), txn_schema)) \
    .select("timestamp", "data.*")

high_value = parsed.filter(F.col("amount") > 1000)

query = high_value.writeStream \
    .format("parquet") \
    .option("path", "/output/high_value") \
    .option("checkpointLocation", "/checkpoints/high_value") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()
```

---

### Q6
`maxOffsetsPerTrigger` limits how many Kafka records Spark fetches **per partition** per micro-batch. Without it: on restart after 6 hours of downtime with a high-volume topic, Spark may try to fetch millions of records in the first batch — causing OOM or extremely long batch times, potentially failing the job. Set it to a reasonable limit (e.g., `100000`) to process the backlog gradually without overwhelming memory.

---

### Q7
The output DataFrame must have `key` (optional, string or binary) and `value` (required, string or binary) columns.

```python
output = enriched_df.select(
    F.col("order_id").cast("string").alias("key"),     # Kafka partition key
    F.to_json(F.struct(
        "order_id", "customer_id", "amount", "category", "tax"
    )).alias("value")    # JSON-serialized payload
)

output.writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "enriched-orders") \
    .option("checkpointLocation", "/checkpoints/kafka_out") \
    .start()
```

---

### Q8
**12 partitions → 12 Spark tasks** per micro-batch. Each task reads one Kafka partition.

When you scale Kafka to 24 partitions while the job is running: Spark detects the new partitions at the next micro-batch (via Kafka consumer group metadata refresh) and automatically creates 24 tasks. No restart needed — Spark dynamically adjusts to the new partition count.

---

### Q9
From weakest to strongest:

1. **No checkpoint, file sink** — at-most-once. On restart, Spark doesn't know where it left off. If the job crashed mid-write, data from that batch is lost.

2. **Checkpoint + Kafka sink** — at-least-once. Checkpoint tracks offsets, so no data is lost. But on retry, already-sent Kafka messages may be resent (no idempotent deduplication in basic Kafka producer).

3. **Checkpoint + `foreachBatch` + idempotent JDBC upsert** — exactly-once. Checkpoint ensures no data is lost. UPSERT on primary key ensures retried writes don't create duplicates.

4. **Checkpoint + Delta sink** — exactly-once. Delta's transactional commits make writes idempotent. If batch N is retried, Delta's transaction log detects the duplicate commit and rejects it.

---

### Q10
On restart after the crash:
1. Spark reads the checkpoint `offsets/` for batch 7 — knows it needs to re-read the source data
2. Spark checks `commits/` — batch 7 is NOT committed
3. Spark re-reads the source data for batch 7
4. Spark re-writes to Parquet (at-least-once behavior for Parquet in some configs, or exactly-once for file sink)

With a **file sink**: Spark writes to a temp path and atomically renames — the second write overwrites the temp file → 1000 rows total (no duplicates).

With **foreachBatch + append**: the second `write.mode("append")` would create duplicates → 2000 rows. You must add idempotency logic using `batch_id`.

---

### Q11
An idempotent write: running it N times with the same input produces the same state as running it once. The second and subsequent invocations are no-ops or produce identical data.

Three examples:
1. **File sink with overwrite**: `write.mode("overwrite").parquet("/output/batch_5")` — second write replaces first with identical data
2. **Delta MERGE with batch_id**: `MERGE INTO target USING source ON target.batch_id = source.batch_id WHEN NOT MATCHED THEN INSERT` — second attempt sees the row already exists and does nothing
3. **JDBC UPSERT on primary key**: `INSERT INTO table (id, amount) VALUES (?,?) ON CONFLICT (id) DO UPDATE SET amount=EXCLUDED.amount` — second insert with same `id` updates to same value (no duplicate row)

---

### Q12
The static `user_profiles` is loaded **once** when the streaming query starts. New users added to the profiles table AFTER the query starts are NOT picked up — the static side is a snapshot from query-start time.

To pick up new profile rows:
- **Option 1**: Restart the streaming query periodically (reload static side at startup)
- **Option 2**: Use `foreachBatch` and re-read the profiles inside each batch function
- **Option 3**: Move the user profiles to a streaming source (Delta CDF or Kafka) and use a stream-stream join

---

### Q13
```python
from pyspark.sql import functions as F

orders   = orders_stream.withWatermark("order_time",   "1 hour")
payments = payments_stream.withWatermark("payment_time", "1 hour")

joined = orders.join(
    payments,
    (orders.order_id == payments.order_id) &
    (payments.payment_time >= orders.order_time) &
    (payments.payment_time <= orders.order_time + F.expr("INTERVAL 30 MINUTES")),
    how="inner"
)
```
The time range constraint `payment_time between order_time and order_time + 30min` limits how long an order row stays buffered in state (evicted if no payment arrives within 30 minutes).

---

### Q14
State size = 500 events/sec × 3600 sec = **1.8M rows on each side** = ~3.6M rows total in state at any moment.

At even 200 bytes per row (typical struct): `3.6M × 200 = 720 MB` just for join state. Plus executor JVM heap, shuffle memory, task memory.

Rule of thumb: your executor memory should be at least `3 × (state size in RAM)` to avoid spill. So: `3 × 720 MB ≈ 2.2 GB` minimum per executor for this join alone.

→ Use the smallest watermark that covers your realistic late-arrival window.

---

### Q15
Five things every production streaming job must have:

1. **`checkpointLocation` on durable storage** (S3/HDFS — not local disk)
2. **Watermark** on all stateful operations (bounded state, no OOM)
3. **`maxOffsetsPerTrigger`** (or `maxFilesPerTrigger`) to control backlog processing rate
4. **Dead letter sink** for malformed/invalid records (never silently drop bad data)
5. **Graceful shutdown handler** (`signal.signal(SIGTERM, lambda: query.stop())`) so the checkpoint is valid on restart
