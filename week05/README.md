# Week 5: DataFrames & Spark SQL — Part 3

> **Phase 3 → Week 5**
>
> Prerequisites: Week 4 complete (Joins, Window Functions, Built-in Functions, Nulls, Complex Types)

---

## What You'll Learn This Week

Week 5 closes out Phase 3 with the tools you need for production-grade Spark code: custom functions, full Spark SQL, reading query plans, caching expensive computations, and writing data efficiently.

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [User-Defined Functions](01_udf.ipynb) | Python UDF, Pandas UDF (vectorized), performance comparison |
| 02 | [Spark SQL & Views](02_spark_sql.ipynb) | createTempView, global views, CTEs, PIVOT, ROLLUP, catalog API |
| 03 | [Catalyst Optimizer](03_catalyst_optimizer.ipynb) | explain plans, predicate pushdown, column pruning, AQE |
| 04 | [Caching & Persistence](04_caching_persistence.ipynb) | cache(), persist(), StorageLevel, checkpoint, when to cache |
| 05 | [Partitioning & Bucketing](05_partitioning_bucketing.ipynb) | repartition vs coalesce, partitionBy, bucketing, optimal sizes |

---

## Key Rules to Remember

### UDFs
- **Order of preference**: built-in functions > Pandas UDF > Python UDF
- Always handle `None` inside every UDF
- Register with `spark.udf.register()` to use in SQL queries
- Pandas UDFs require `spark.sql.execution.arrow.pyspark.enabled = true`

### Spark SQL
- SQL and DataFrame API produce **identical physical plans** — same performance
- `createOrReplaceTempView` is safer than `createTempView` (no exception on re-run)
- Global temp views use the `global_temp.` prefix
- SQL supports: CTEs, window functions, PIVOT, ROLLUP, CUBE, subqueries

### Catalyst Optimizer
- Read explain() output **bottom-up**
- Look for `PushedFilters` (predicate pushdown good), `BroadcastHashJoin` (good), `SortMergeJoin` (expensive)
- AQE auto-coalesces shuffle partitions and converts joins at runtime
- You don't need to write filters early — Catalyst moves them for you

### Caching
- `cache()` is **lazy** — must trigger an action to materialize
- Always call `unpersist()` when done — never let cache accumulate
- Default: `MEMORY_AND_DISK` (spills to disk if RAM full)
- Use `checkpoint()` to break lineage in deep/iterative pipelines

### Partitioning
- `repartition(N)` = full shuffle, even, can increase or decrease
- `coalesce(N)` = no shuffle, can only decrease, may be uneven
- `partitionBy("col")` on write = folder per value = partition pruning on read
- Never partition on high-cardinality columns (millions of unique values)
- Bucketing eliminates join shuffle when both sides bucketed on same key

---

## Interview Questions Covered

1. What is a UDF? What are its downsides?
2. What is a Pandas UDF and why is it faster?
3. What's the difference between createTempView and createOrReplaceTempView?
4. Does Spark SQL perform the same as the DataFrame API?
5. What is the Catalyst Optimizer?
6. What is predicate pushdown?
7. What is AQE?
8. How do you read an explain() plan?
9. What's the difference between cache() and persist()?
10. Is cache() immediate?
11. What's the difference between repartition() and coalesce()?
12. What is storage partitioning and how does partition pruning work?
13. What is bucketing and when should you use it?

---

## Phase 3 Complete!

After Week 5, you've mastered the full DataFrame & Spark SQL API. You can:
- Build multi-table analytical queries with joins and window functions
- Write efficient UDFs using the right tool for each job
- Use SQL or the DataFrame API interchangeably
- Read and interpret query execution plans
- Cache intelligently to avoid recomputation
- Write data efficiently for downstream query performance

**Next up → Phase 4: Performance & Optimization (Weeks 6-7)**
- Week 6: Memory Management, GC tuning, Executor configuration
- Week 7: Shuffles deep-dive, Skew, AQE advanced, Spark UI mastery

--

## How to Run

```bash
docker-compose up -d
# Open http://localhost:8888
# Navigate to week5/ and run notebooks in order
```
