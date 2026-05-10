# Week 11: Spark Structured Streaming — Part 2

> **Phase 6 → Week 11**
>
> Prerequisites: Week 10 (Streaming basics, watermarks, stateful aggregations)

---

## What You'll Learn This Week

Week 11 builds on the streaming foundations from Week 10. You go from basic streaming concepts to production-ready patterns: time-based window aggregations, Kafka integration, exactly-once guarantees, stream-stream joins, and everything needed to ship a streaming job to production.

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [Window Operations](01_window_operations.ipynb) | Tumbling, sliding, session windows with watermark |
| 02 | [Kafka Integration](02_kafka_integration.ipynb) | Reading/writing Kafka, parsing binary values, options |
| 03 | [Exactly-Once Semantics](03_exactly_once.ipynb) | Delivery guarantees, idempotent writes, checkpoint recovery |
| 04 | [Streaming Joins](04_streaming_joins.ipynb) | Stream-static enrichment, stream-stream join, watermark join |
| 05 | [Production Streaming](05_production_streaming.ipynb) | Dead letter, monitoring, schema evolution, deployment template |

---

## Key Rules to Remember

### Window Operations
- `F.window("ts", "10 minutes")` → tumbling: each event in exactly one window
- `F.window("ts", "10 minutes", "5 minutes")` → sliding: each event in `size/slide` windows
- `session_window("ts", "5 minutes")` → session: close window after 5 min of inactivity (Spark 3.2+)
- Always `withWatermark()` BEFORE `groupBy(F.window(...))` — required for bounded state
- Window results: `col("window.start")` and `col("window.end")`
- `update` mode: partial results emitted as data arrives (low latency)
- `append` mode: results emitted only after window is closed by watermark (no late changes)

### Kafka Integration
- Spark presents Kafka messages as: `key (binary), value (binary), topic, partition, offset, timestamp`
- Always cast `value` to string, then `from_json()` to parse the payload
- `startingOffsets`: matters only on the FIRST run (no checkpoint). Use `"latest"` in production
- `maxOffsetsPerTrigger`: limits records per partition per batch — prevent OOM on large backlogs
- Add Kafka connector: `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`
- Write to Kafka: requires `key` (string/binary) and `value` (string/binary) columns

### Exactly-Once Semantics
- Three requirements: replayable source + durable checkpoint + idempotent sink
- File/Delta sink: exactly-once out of the box
- Kafka sink: at-least-once (need transactional producer for exactly-once)
- `foreachBatch`: exactly-once IF your batch function is idempotent
- Idempotent write: same input always produces same output (overwrite, MERGE, upsert)
- On restart with checkpoint: Spark re-reads the uncommitted batch and re-applies idempotent write

### Streaming Joins
- Stream-static: stateless, any join type, append output mode, no watermark needed
- Stream-stream: stateful, requires watermark on BOTH streams, append output mode
- Stream-stream state size = (rows/sec) × (watermark duration) — size both sides
- Add time range constraint to stream-stream join to further bound state
- Left outer stream-stream: unmatched left rows emitted with null right columns AFTER watermark passes them
- Never join two unbounded streams without watermarks — OOM guaranteed

### Production Streaming
- Always `signal.signal(SIGTERM)` → `query.stop()` for graceful shutdown
- Schema evolution: use nullable field additions + `from_json` (permissive by default)
- Monitor `lastProgress.inputRowsPerSecond` vs `processedRowsPerSecond` — falling behind = alert
- Dead letter: always route bad records to a separate sink (never silently drop)
- Checkpoint on S3/HDFS — local disk checkpoint dies when the executor is rescheduled
- Use `maxOffsetsPerTrigger` to control backlog processing rate

---

## Interview Questions Covered

1. What is the difference between tumbling and sliding windows?
2. What is a session window and when would you use it?
3. How does Spark read from Kafka? What does the schema look like?
4. What is `startingOffsets` and when is it used vs ignored?
5. What are the three delivery guarantees? Which does Spark Structured Streaming provide?
6. How does Spark achieve exactly-once with file sinks?
7. Why is the Kafka sink only at-least-once?
8. What is the difference between stream-static and stream-stream joins?
9. Why do stream-stream joins require watermark on BOTH streams?
10. How do you handle schema evolution in a streaming pipeline?
11. How do you gracefully shut down a streaming query?
12. What fields in `lastProgress` do you monitor in production?

---

## How to Run

```bash
docker-compose up -d
# Open http://localhost:8888
# Navigate to week11/ and run notebooks in order
```

> **Kafka note:** Notebook 02 shows Kafka patterns that require a running Kafka broker. All other notebooks use the built-in `rate` or file sources and run without external dependencies.
