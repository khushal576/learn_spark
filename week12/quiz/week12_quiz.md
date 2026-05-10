# Week 12 Quiz — Phase 7: Advanced & Master Level

> This is the capstone quiz. Cover everything from all 12 weeks.

---

## Section 1: Cluster Configuration (6 points)

**Q1.** You have a cluster: 10 nodes, 16 cores each, 64 GB RAM each. Calculate `--executor-cores`, `--executor-memory`, `--num-executors`, and the approximate `memoryOverhead`. Show your work step by step.

**Q2.** Your PySpark job fails with "Cannot allocate memory in static TLS block" on the executors. Which configuration parameter do you increase and why? What does this parameter cover that `spark.executor.memory` does not?

**Q3.** Explain the Spark unified memory model: what is `spark.memory.fraction`, what are execution and storage memory, and how do they borrow from each other?

---

## Section 2: Advanced Joins (8 points)

**Q4.** Spark plans a Sort-Merge Join between two tables. The smaller table is 8 MB. How do you force Spark to use a Broadcast Hash Join? Write both the hint and the config approach. What is the difference?

**Q5.** You have a join where `category='electronics'` represents 90% of rows in the orders table. The Spark UI shows one task is running for 30 minutes while all others finished in 20 seconds. Diagnose and fix using salting. Show the code for both sides of the join.

**Q6.** What is a Bloom Filter join? When does it help and when doesn't it? Is it available by default in Spark 3.x?

**Q7.** Compare BHJ, SMJ, and SHJ (Shuffle Hash Join). Fill in the table:

| Algorithm | When used | Shuffles? | Sorts? | Best for |
|-----------|-----------|-----------|--------|---------|
| BHJ | ? | ? | ? | ? |
| SMJ | ? | ? | ? | ? |
| SHJ | ? | ? | ? | ? |

---

## Section 3: Pandas UDFs & Arrow (6 points)

**Q8.** Write a scalar Pandas UDF called `safe_log` that computes `log(x + 1)` for positive values and returns 0.0 for null or non-positive values. Show the decorator, type annotation, and pandas implementation.

**Q9.** You need to run a scikit-learn model inference on each row in a streaming DataFrame. The model is 200 MB and takes 10 seconds to load. Which Pandas UDF type do you use and why? Show the skeleton code.

**Q10.** What does `spark.sql.execution.arrow.pyspark.enabled=true` do? Name two operations it specifically accelerates.

---

## Section 4: Cloud Platforms (4 points)

**Q11.** Your team needs to run a nightly 2-hour Spark job on 50 TB of S3 data. You are on AWS. Compare EMR + Spot instances vs AWS Glue for this use case. Which do you recommend and why?

**Q12.** What is the Glue Data Catalog? Why is it useful in a multi-service AWS architecture with Athena, EMR, and Redshift?

---

## Section 5: Interview Masterclass — Comprehensive (16 points)

**Q13.** Explain Spark's lazy evaluation. What is the difference between a transformation and an action? Give two examples of each.

**Q14.** You have a DataFrame with 200 shuffle partitions. You write it to S3 — how many files are created? How do you reduce this to 10 files without a full reshuffle? What is the trade-off?

**Q15.** Describe the complete flow of a Sort-Merge Join in Spark: what happens in the driver, what happens in each executor, and what data crosses the network.

**Q16.** You need to find, for each customer, their most recent order in the last 30 days. Write the PySpark code using Window functions.

**Q17. (System Design)** Design a Lambda Architecture (batch + streaming) for a ride-sharing company. Requirements: real-time driver location tracking (10K updates/sec), hourly revenue reports per city, historical trip analysis for 3 years. Describe: sources, Kafka topics, Spark streaming jobs, batch jobs, storage layers (Delta), and serving layer. What consistency tradeoffs exist between the batch and streaming layers?

**Q18.** List 5 common Spark performance mistakes and how you would fix each one.

---

## Answers

### Q1
Step-by-step executor sizing:
- **Cores per executor**: 5 (rule of thumb for HDFS/S3 throughput)
- **Executors per node**: floor((16 cores - 1 for OS) / 5) = floor(15/5) = **3 executors/node**
- **Total executor slots**: 10 nodes × 3 = 30 total
- **Driver takes 1**: 30 - 1 = **29 executors** (`--num-executors 29`)
- **RAM per executor**: floor((64 GB - 1 GB OS) / 3) = floor(63/3) = **21 GB**
- **memoryOverhead**: max(21 × 0.1, 0.384) = **2.1 GB**
- **executor.memory**: 21 - 2.1 = **~19 GB** (`--executor-memory 19g`)
- **executor.cores**: 5

Final flags:
```
--executor-cores 5
--executor-memory 19g
--num-executors 29
--conf spark.executor.memoryOverhead=2g
```

---

### Q2
Increase `spark.executor.memoryOverhead`. This parameter allocates off-heap memory for: JVM overhead (code cache, metaspace), Python worker processes (for PySpark UDFs), NIO buffers, native library loading (BLAS, Arrow), and other non-JVM memory consumers.

`spark.executor.memory` only covers the JVM heap. PySpark UDFs run in a separate Python subprocess — that process's memory comes from `memoryOverhead`, not from `executor.memory`. If the Python worker runs out of memory (large UDFs, Arrow batches), you get "Cannot allocate memory" at the OS level.

Fix: `--conf spark.executor.memoryOverhead=4g`

---

### Q3
Spark Unified Memory Model:

**`spark.memory.fraction`** (default 0.6): fraction of `(executor.memory - 300 MB)` allocated to Spark's unified memory pool. The rest is "user memory" (UDFs, Python objects, Spark internals not managed by the pool).

**Execution memory**: used for shuffle, sort, aggregations, and join hash tables. Spills to disk when full.

**Storage memory**: used for cached/persisted DataFrames and broadcast variables.

**Borrowing**: The two regions share the unified pool. If execution needs more memory and storage has free pages, execution can borrow them (evicting cached blocks if necessary). Storage can also borrow from execution when execution is idle. The `spark.memory.storageFraction` sets the minimum storage fraction that execution cannot evict (default 0.5 of the unified pool).

This dynamic borrowing means: if you don't cache anything, execution gets almost the full pool; if you cache heavily, it gets less.

---

### Q4
**Config approach** (session-wide, affects all joins):
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(20 * 1024 * 1024))  # raise to 20 MB
```

**Hint approach** (per-query, overrides optimizer decision):
```python
result = large_df.join(small_df.hint("broadcast"), "key")
# or:
result = large_df.join(F.broadcast(small_df), "key")
```

Difference: the config approach raises the automatic threshold globally — affects ALL queries in the session. The hint approach forces BHJ for just this specific join, even if `small_df` is above the auto-threshold. Hints are more precise and don't affect other joins.

---

### Q5
Diagnosis: task skew (one task takes 100× longer → that partition has ~90% of all data).

Fix with salting:
```python
SALT = 5  # number of sub-buckets

# Salt the large (skewed) table: distribute 'electronics' across 5 buckets
salted_orders = orders \
    .withColumn("salt", (F.rand() * SALT).cast("int")) \
    .withColumn("salted_cat", F.concat(F.col("category"), F.lit("_"), F.col("salt").cast("string")))

# Expand the small (lookup) table: replicate each category N times
salted_cats = categories \
    .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(SALT)]))) \
    .withColumn("salted_cat", F.concat(F.col("category"), F.lit("_"), F.col("salt").cast("string")))

# Join on the salted key
result = salted_orders \
    .join(F.broadcast(salted_cats), "salted_cat") \
    .drop("salt", "salted_cat")
```

The `electronics` category is now split across 5 buckets, each processed by a separate task. Peak task time ≈ 30 min / 5 = 6 min.

---

### Q6
A Bloom Filter is a probabilistic data structure with two guarantees:
- **No false negatives**: if an element is definitely NOT in the set, the filter returns "no."
- **Small false positive rate**: the filter may occasionally return "yes" for elements not in the set (< 1% rate configurable).

In a join, AQE (Spark 3.3+) builds a Bloom Filter from the join keys of the smaller table, then uses it to filter rows in the larger table BEFORE the shuffle. Rows whose key is "definitely not in the small table" are dropped early, reducing shuffle data.

**When it helps**: high-selectivity joins where most rows in the large table don't match. Example: 1B events joined with 10K VIP users — 99%+ events are filtered before shuffle.

**When it doesn't help**: when most rows match (low selectivity), or when the table is already small enough for BHJ.

**Default**: not enabled by default. Enable with:
```
spark.sql.optimizer.runtime.bloomFilter.enabled=true
```

---

### Q7
| Algorithm | When used | Shuffles? | Sorts? | Best for |
|-----------|-----------|-----------|--------|---------|
| BHJ | One side < threshold (10 MB) | No (broadcasts small side) | No | Dimension table joins, lookups |
| SMJ | Both sides large | Yes (both sides) | Yes (after shuffle) | Large-large table joins (most common) |
| SHJ | Smaller side fits in memory after shuffle | Yes (both sides) | No | Medium tables, AQE switches SMJ→SHJ |

---

### Q8
```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd
import numpy as np

@pandas_udf(DoubleType())
def safe_log(x: pd.Series) -> pd.Series:
    return x.apply(lambda v: float(np.log(v + 1)) if v is not None and v > 0 else 0.0)

# More vectorized (faster):
@pandas_udf(DoubleType())
def safe_log_v2(x: pd.Series) -> pd.Series:
    result = pd.Series(0.0, index=x.index)
    mask = x.notna() & (x > 0)
    result[mask] = np.log(x[mask] + 1)
    return result
```

---

### Q9
Use an **Iterator Pandas UDF** — it loads the model once per executor partition and applies it to all Arrow batches:

```python
from typing import Iterator
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def score_with_model(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # Runs ONCE per executor partition (not once per row/batch)
    import joblib
    model = joblib.load("/dbfs/models/fraud_detector.pkl")  # expensive: 10s, 200 MB

    for batch in iterator:    # process each Arrow batch
        features = batch.values.reshape(-1, 1)
        yield pd.Series(model.predict_proba(features)[:, 1])
```

If you used a regular Pandas UDF, the model would be reloaded for EVERY Arrow batch (potentially thousands of times per executor). The Iterator pattern loads it once and reuses it for all batches sent to that executor.

---

### Q10
`spark.sql.execution.arrow.pyspark.enabled=true` enables Apache Arrow for data transfer between the Spark JVM and the Python process. Arrow uses a columnar memory layout that can be shared across language boundaries with minimal copying (zero-copy in many cases).

Two operations it accelerates:
1. **`df.toPandas()`**: instead of serializing row-by-row with pickle, Spark serializes entire columns as Arrow arrays → 5-10× faster for DataFrames with many rows.
2. **`spark.createDataFrame(pandas_df)`**: converts a pandas DataFrame to a Spark DataFrame using Arrow column transfer instead of row-by-row pickle serialization → 5-10× faster.

Pandas UDFs also rely on Arrow for their batch transfers.

---

### Q11
**Recommendation: EMR + Spot instances.**

Reasons:
- A 2-hour batch job on 50 TB benefits from full resource control (custom executor sizing for the data volume)
- Spot instances reduce EC2 cost 60-80% for a job that is restartable
- EMR integrates directly with S3 and Glue Catalog
- Glue has per-DPU-hour pricing and limited performance tuning — at 50 TB scale, you'd need many DPUs at high cost with no control over shuffle partitions, executor memory, or AQE settings
- Glue doesn't support large-scale Spark performance optimizations well (managed = constrained)

When Glue wins: small ETL jobs (< 1 TB), zero cluster management budget, tight AWS integration requirements, short development timeline.

---

### Q12
The Glue Data Catalog is a centralized metadata repository that stores table schemas, partition information, and connection details for data stored in S3 (and other sources). It acts as a Hive-compatible metastore.

Multi-service value:
- **Athena**: queries S3 data using Glue Catalog table definitions without data movement
- **EMR**: uses Glue Catalog as Hive metastore (same table names, schemas as Athena)
- **Redshift Spectrum**: queries S3 via Glue Catalog as external tables
- **Glue ETL**: reads/writes using Catalog — schema changes propagate everywhere

One schema definition → all services see the same table → no duplicate schema management. When a Glue Crawler discovers new S3 partitions, all services automatically see the new data.

---

### Q13
**Lazy evaluation**: Spark builds a logical plan (DAG) when you call transformations, but does NOT execute anything. Execution is deferred until you call an action.

**Transformation**: returns a new DataFrame/RDD. Lazy — adds to the plan, doesn't run.
- Examples: `filter(col > 100)`, `withColumn("tax", col("amount") * 0.18)`, `groupBy("key").count()`, `join(other, "key")`

**Action**: triggers execution of the entire DAG and returns a result or writes output.
- Examples: `count()` → Long, `collect()` → List[Row], `show()` → prints, `write.parquet(path)` → writes files

Why lazy: Catalyst can see the full pipeline before running, enabling optimizations like predicate pushdown (push filter before join), constant folding, and projection pruning (drop unused columns early).

---

### Q14
Default 200 shuffle partitions → **200 output files** (one per partition).

Reduce to 10 files using **coalesce**:
```python
df.coalesce(10).write.mode("overwrite").parquet("s3://bucket/output/")
```

Trade-off: `coalesce(10)` is a narrow transformation — it combines partitions without a full shuffle. But it can create uneven partitions (some tasks process 20× more data than others). If the data was already evenly distributed after the shuffle, this is fine. If you need exactly even distribution, use `repartition(10)` (full shuffle — more expensive but perfectly even output).

---

### Q15
Sort-Merge Join execution flow:

**Driver**:
1. Catalyst analyzes both tables and determines SMJ is the right algorithm
2. Creates a physical plan with two Exchange (shuffle) nodes, one per table
3. Schedules Stage 1: scan + shuffle both tables

**Stage 1 (Map phase)**:
- Each executor reads its input partitions for both tables
- For each row, computes `hash(join_key) % num_partitions` → routing key
- Writes rows to local shuffle output files, partitioned by routing key
- Data crosses the network: each executor's shuffle output is pulled by all other executors

**Stage 2 (Sort phase)**:
- Each executor pulls all rows for its partition bucket from all Stage 1 executors
- Sorts both sides by join key within its partition

**Stage 3 (Merge phase)**:
- Each executor does a merge join (like merge-sort): walks both sorted lists simultaneously
- Emits matched rows, advances pointers

Network data: all rows from both tables cross the network once (sorted by join key). The shuffle is the bottleneck — why BHJ (no shuffle for small table) is so much faster.

---

### Q16
```python
from pyspark.sql import Window
from pyspark.sql import functions as F
from datetime import date, timedelta

thirty_days_ago = F.date_sub(F.current_date(), 30)

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())

result = orders \
    .filter(F.col("order_date") >= thirty_days_ago) \
    .withColumn("_rn", F.row_number().over(w)) \
    .filter(F.col("_rn") == 1) \
    .drop("_rn") \
    .select("customer_id", "order_id", "order_date", "amount", "status")
```

---

### Q17
**Lambda Architecture for Ride-Sharing:**

**Sources:**
- Driver GPS → Kafka topic `location-updates` (10K msgs/sec, JSON: driver_id, lat, lng, ts)
- Trip completed → Kafka topic `trips-completed` (JSON: trip_id, driver_id, rider_id, fare, city, ts)

**Streaming Layer (Spark Structured Streaming):**
- Job 1 (location tracking):
  - Source: `location-updates`, watermark 30s
  - Tumbling 1-min window: last known position per driver
  - Sink: Redis (for real-time driver map in app) + Bronze Delta table
  - Trigger: processingTime='10 seconds'

- Job 2 (revenue streaming):
  - Source: `trips-completed`, watermark 5 min
  - Tumbling 1-hour window: revenue by city
  - Sink: Delta streaming table `gold.hourly_revenue_stream`
  - Trigger: processingTime='1 minute'

**Batch Layer (nightly, EMR):**
- Bronze → Silver: validate trip schema, deduplicate (trip_id), fill missing cities from driver profiles
- Silver → Gold:
  - Daily revenue per city/driver/hour
  - SCD2 driver table (name, vehicle, tier history)
  - 30-day cohort retention (first trip date → did they complete week 2?)
  - OPTIMIZE + ZORDER BY (city, trip_date) on Gold tables

**Storage:**
- Bronze: `s3://data/bronze/trips/` (Delta, date-partitioned)
- Silver: `s3://data/silver/trips/` (Delta, deduped, date-partitioned)
- Gold batch: `s3://data/gold/daily_revenue/` (Delta)
- Gold streaming: `s3://data/gold/hourly_revenue_stream/` (Delta, rolling 7 days)

**Serving:**
- BI dashboards → query Gold batch Delta tables
- Real-time operations → query Gold streaming table or Redis
- Ad-hoc analysis → Athena on Silver via Glue Catalog

**Consistency tradeoffs (Lambda):**
- Streaming layer provides fast (1-min) approximate results
- Batch layer provides correct (24h latency) results
- If streaming and batch disagree (late data, reprocessing): batch wins
- Kappa Architecture alternative: use only streaming (simpler, single code path, but harder to reprocess history)

---

### Q18
5 Common Spark Performance Mistakes + Fixes:

1. **Not using broadcast joins** — joining a 5 MB lookup table with a 500 GB orders table triggers an unnecessary sort-merge join. Fix: `F.broadcast(small_df)` or raise `autoBroadcastJoinThreshold`.

2. **Using Python UDFs for math operations** — 10-100× slower than built-in `F.*` functions due to JVM↔Python pickle serialization. Fix: use `F.when()`, `F.log()`, `F.round()`, etc. For custom logic: use Pandas UDF instead.

3. **No partition control on output writes** — default 200 shuffle partitions → 200 files. Fix: `coalesce(N)` before write, or set `spark.sql.shuffle.partitions` to match the workload.

4. **Forgetting to unpersist cached DataFrames** — cached DFs hold executor memory indefinitely, starving other jobs. Fix: `df.unpersist()` after the section that needs the cache.

5. **Data skew without AQE or salting** — one task runs 100× longer than others, making the whole job wait. Fix: enable AQE (`spark.sql.adaptive.skewJoin.enabled=true`) or manually salt the skewed join key.
