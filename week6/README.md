# Week 6: Performance & Optimization — Memory Management

> **Phase 4 → Week 6**
>
> Prerequisites: Phase 3 complete (Weeks 3-5: DataFrames, Spark SQL, UDFs, Catalyst, Caching, Partitioning)

---

## What You'll Learn This Week

Phase 4 shifts from "how to use Spark" to "how to run Spark fast." Week 6 covers the internals of how Spark uses memory — understanding this is critical for diagnosing and fixing the most common production issues.

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [Spark Memory Model](01_spark_memory_model.ipynb) | Unified Memory Manager, execution/storage/user/reserved/off-heap regions |
| 02 | [Executor Configuration](02_executor_configuration.ipynb) | Driver vs executor, sizing calculator, dynamic allocation, spark-submit |
| 03 | [GC & Serialization](03_gc_and_serialization.ipynb) | G1GC, Kryo, reducing GC pressure, serialization comparison |
| 04 | [Spill & OOM Diagnosis](04_spill_and_oom.ipynb) | Spill mechanics, OOM patterns, diagnosis workflow, fixes |
| 05 | [Broadcast Variables & Accumulators](05_broadcast_accumulators.ipynb) | `sc.broadcast()`, shared read-only data, counters, gotchas |

---

## Key Rules to Remember

### Memory Model
- Spark memory pool = `(heap - 300MB) × memory.fraction` (default 60%)
- Execution and Storage share the pool and can borrow from each other
- Execution memory evicts Storage (cache) — never the other way
- Off-heap: Tungsten binary buffers, no GC overhead
- Total physical memory = `executor.memory` + overhead (10% or min 384MB)

### Executor Sizing
- Sweet spot: 4-5 cores per executor, 8-16GB memory
- Never exceed node RAM: always account for overhead
- `shuffle.partitions` sweet spot = 2-4× total executor cores
- Leave 1 core + 1GB per node for OS/YARN/HDFS daemons

### GC
- G1GC (`-XX:+UseG1GC`) is better than default for large heaps
- `InitiatingHeapOccupancyPercent=35` = start GC earlier, shorter pauses
- GC > 10% of task time = problem; enable off-heap to reduce it
- Serialized caching (`MEMORY_ONLY_SER`) reduces GC pressure

### Spill & OOM
- Spill = partial results written to disk when execution memory full → slow but safe
- OOM = heap exhausted with no spill path → task/executor dies
- Fix spill: **more partitions** (smaller data per task)
- Fix OOM: diagnose location (driver vs executor), then fix root cause
- Never `collect()` large DataFrames — write to storage instead

### Broadcast & Accumulators
- `sc.broadcast(data)`: sent ONCE per executor (not per task) — use for large lookups
- `F.broadcast(df)`: hint to Catalyst for BroadcastHashJoin
- Accumulators: write-only from tasks, read-only from driver
- Accumulators NOT guaranteed exactly-once in transformations — use in actions

---

## Interview Questions Covered

1. What are the memory regions in a Spark executor?
2. What is the Unified Memory Manager?
3. What is Tungsten / off-heap memory?
4. How many cores per executor is optimal and why?
5. What is `spark.sql.shuffle.partitions` and how do you size it?
6. What is G1GC and why is it preferred for Spark?
7. What is Kryo serialization?
8. What is spill in Spark?
9. How do you diagnose an OOM in Spark?
10. What is a broadcast variable?
11. What is an accumulator and what are its limitations?

---

## How to Run

```bash
docker-compose up -d
# Open http://localhost:8888
# Navigate to week6/ and run notebooks in order
```
