# Week 7: Shuffles, Skew & Spark UI Mastery

> **Phase 4 → Week 7**
>
> Prerequisites: Week 6 complete (Memory Model, Executor Config, GC, Spill, Broadcast)

---

## What You'll Learn This Week

Week 7 completes Phase 4 — the performance engineering phase. You'll understand why shuffles are expensive, how to detect and fix data skew, how Adaptive Query Execution works internally, and how to navigate the Spark UI like an expert to diagnose any performance problem.

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [Shuffle Internals](01_shuffle_internals.ipynb) | Sort-Merge Shuffle, write/read phases, Exchange nodes, reducing shuffles |
| 02 | [Data Skew Deep Dive](02_data_skew.ipynb) | Detecting skew, salting technique, AQE skew join, null key skew |
| 03 | [AQE Advanced](03_aqe_advanced.ipynb) | Partition coalescing, dynamic join strategy, skew join config |
| 04 | [Spark UI Mastery](04_spark_ui_mastery.ipynb) | Jobs/Stages/Tasks/SQL/Executors/Storage tabs, systematic diagnosis |
| 05 | [Optimization Patterns & Checklist](05_optimization_patterns.ipynb) | Anti-patterns, positive patterns, production checklist |

---

## Key Rules to Remember

### Shuffle
- Every `groupBy`, `join` (SMJ), `distinct`, `repartition`, `orderBy` triggers a shuffle
- Shuffle = stage boundary = ALL upstream tasks must finish before downstream starts
- Each Exchange node in `explain()` = one shuffle = one stage boundary
- Reduce shuffles: broadcast small tables, combine operations, filter before shuffle

### Data Skew
- Skew = one partition has 10x+ more data than others → one task blocks the whole stage
- Detect: `df.groupBy("key").count().orderBy(F.col("count").desc())`
- Fix order: (1) broadcast small side → (2) AQE skew join → (3) salting → (4) filter+union
- Salting: add random suffix 0..N to hot key (left), replicate right side N times, re-aggregate after
- NULL keys: all go to the same partition — coalesce or salt null keys

### AQE
- AQE re-optimizes the plan at runtime after each shuffle stage
- Three features: coalesce tiny partitions, switch SMJ→BHJ, split skewed partitions
- Enable it: `spark.sql.adaptive.enabled=true` (default in Spark 3.2+)
- `shuffle.partitions` becomes a maximum, not an exact count — AQE reduces it

### Spark UI Diagnosis
- Slow stage? → Stage Detail → Tasks → sort by Duration → look for outlier task
- Spill (Disk) > 0 → increase shuffle.partitions
- One task >> others → skew (check Input Size / Shuffle Read Size)
- GC Time > 10% of Duration → GC pressure → G1GC + MEMORY_ONLY_SER
- SQL tab → count Exchange nodes → look for SortMergeJoin on small tables

### Optimization Priority
1. **Design** (highest impact): partitioning, bucketing, data format, broadcast strategy
2. **Query logic**: filter early, select only needed columns, combine operations, cache reused DFs
3. **Config** (tuning): shuffle.partitions, executor sizing, AQE, GC flags

---

## Production Checklist Summary

```
Config:     AQE on, shuffle.partitions = 2-4× cores, G1GC, executor 4-5 cores
Code:       project early, filter before shuffle, broadcast small tables
            combine groupBys, built-ins > UDFs, cache + unpersist
UI check:   spill, skew (task outlier), GC%, exchange count
Write:      Parquet/Delta, partitionBy, coalesce, never collect() large data
```

---

## Interview Questions Covered

1. What is a shuffle in Spark and why is it expensive?
2. What triggers a shuffle? What does NOT trigger a shuffle?
3. How many shuffle files does Sort-Merge shuffle create?
4. What is data skew and how do you detect it?
5. What is salting and how does it fix skew?
6. What are the three features of AQE?
7. What is partition coalescing in AQE?
8. How do you navigate the Spark UI to diagnose a slow job?
9. What metrics in Spark UI indicate a problem and what do they mean?
10. What are 5 Spark anti-patterns?
11. When should you NOT broadcast a table?

---

## How to Run

```bash
docker-compose up -d
# Open http://localhost:8888
# Navigate to week7/ and run notebooks in order
# Keep Spark UI open at http://localhost:4040 while running Topic 4
```
