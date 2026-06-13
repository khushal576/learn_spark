# Week 5 Quiz — DataFrames & Spark SQL Part 3

> Answer without looking at the notebooks. Check answers afterward.

---

## Section 1: UDFs (8 points)

**Q1.** You have a Python function that calls an external REST API for each row. Which UDF type should you use (Python UDF or Pandas UDF) and why?

**Q2.** Why does this UDF fail on some rows?
```python
@udf(IntegerType())
def string_length(s):
    return len(s)
```
How do you fix it?

**Q3.** A colleague says "I'll just write a Python UDF instead of learning the built-in functions — same result." What do you say?

**Q4.** What is Apache Arrow and why does it make Pandas UDFs faster than Python UDFs?

**Q5.** Write a Pandas UDF that accepts a `pd.Series` of prices and returns a `pd.Series` with prices rounded to the nearest 5 (e.g., 47 → 45, 53 → 55).

---

## Section 2: Spark SQL & Views (8 points)

**Q6.** What is the difference between `createTempView("v")` and `createOrReplaceTempView("v")`? Which should you use in production notebooks?

**Q7.** A data scientist runs this in their notebook:
```python
df.createOrReplaceTempView("data")
result = spark.sql("SELECT * FROM data WHERE date > '2023-01-01'")
```
A few cells later, a different notebook (in the same Spark application) tries `spark.sql("SELECT * FROM data")`. Will it work? Why or why not?

**Q8.** What is a Global Temp View? How do you access it in SQL?

**Q9.** Convert this SQL to DataFrame API code:
```sql
WITH high_spenders AS (
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id
    HAVING SUM(amount) > 1000
)
SELECT c.name, h.total
FROM customers c
JOIN high_spenders h ON c.customer_id = h.customer_id
ORDER BY h.total DESC
```

---

## Section 3: Catalyst Optimizer (8 points)

**Q10.** Read this explain() output and describe what's happening (bottom-up):
```
== Physical Plan ==
*(2) HashAggregate(keys=[category#15], functions=[sum(amount#10)])
+- Exchange hashpartitioning(category#15, 200)
   +- *(1) HashAggregate(keys=[category#15], functions=[partial_sum(amount#10)])
      +- *(1) Project [category#15, amount#10]
         +- *(1) Filter (isnotnull(amount#10) AND (amount#10 > 500.0))
            +- FileScan parquet [amount#10,category#15] PushedFilters: [IsNotNull(amount), GreaterThan(amount,500.0)]
```

**Q11.** What's the difference between `BroadcastHashJoin` and `SortMergeJoin` in an explain plan?

**Q12.** You filter a large DataFrame after joining it to a small lookup table. Describe TWO optimizations Catalyst automatically applies.

**Q13.** What does AQE's "auto coalescing shuffle partitions" feature do? When is it useful?

---

## Section 4: Caching (8 points)

**Q14.** This code caches a DataFrame but the second action is still slow. Why?
```python
df.cache()
df.filter(F.col("status") == "active").show()   # slow
df.groupBy("dept").count().show()               # also slow
```

**Q15.** What is the default StorageLevel for `df.cache()`? When would you use `StorageLevel.MEMORY_ONLY` instead?

**Q16.** Explain the difference between `df.cache()` and `df.checkpoint()`. When should you use checkpoint?

**Q17.** Is there a memory leak in this code? How would you fix it?
```python
for date in date_range:
    daily_df = raw.filter(F.col("date") == date).cache()
    process(daily_df)
```

---

## Section 5: Partitioning & Bucketing (8 points)

**Q18.** You run `df.coalesce(100)` but the result still has only 4 partitions (same as before). Why?

**Q19.** What is "partition pruning" and how does `partitionBy` enable it?

**Q20.** You write a DataFrame with `partitionBy("user_id")` where user_id has 5 million unique values. What problem does this cause?

**Q21.** When joining two large tables on `customer_id`, what optimization can you make at write time to avoid shuffles at join time? Describe the approach.

**Q22.** After a filter that removes 95% of rows, you have 200 almost-empty partitions. Should you use `coalesce()` or `repartition()` to reduce them? Why?

---

## Answers

<details>
<summary>Click to reveal answers</summary>

**A1.** Python UDF. The external REST API call must happen for each individual row — you cannot vectorize it with Pandas. There's no batch API call possible, so you can't benefit from Arrow batch transfer. Python UDF is your only option here. (Mitigation: use connection pooling inside the UDF.)

**A2.** `len(None)` raises `TypeError` in Python. Fix: `return len(s) if s is not None else None`. Or add a null guard: `if s is None: return 0`.

**A3.** "Same result, but 10-100x slower. Python UDFs serialize each row from JVM to Python and back. Built-in functions run natively in Tungsten — no serialization overhead, and Catalyst can optimize them. For simple transformations (string ops, date math, conditionals), always use built-ins."

**A4.** Apache Arrow is a columnar in-memory data format that both JVM and Python can read without copying. Pandas UDFs transfer an entire partition as an Arrow buffer in one round-trip, instead of serializing rows one at a time. The function then operates on the Arrow-backed Pandas Series using vectorized NumPy operations — much faster than row-by-row Python.

**A5.**
```python
@pandas_udf(DoubleType())
def round_to_5(prices: pd.Series) -> pd.Series:
    return (prices / 5).round() * 5
```

**A6.** `createTempView` raises `AnalysisException` if a view with that name already exists. `createOrReplaceTempView` silently replaces it. Use `createOrReplaceTempView` in production — notebooks get re-run and you don't want crashes on the second run.

**A7.** No. Temp views are scoped to the **current SparkSession**. A different session (or notebook kernel using its own session) cannot see temp views from another session. Use `createOrReplaceGlobalTempView("data")` and query it as `SELECT * FROM global_temp.data` if cross-session access is needed.

**A8.** A Global Temp View is visible to all SparkSessions within the same Spark application (same JVM). It lives in the `global_temp` database. Access it with: `SELECT * FROM global_temp.view_name`.

**A9.**
```python
from pyspark.sql.window import Window

high_spenders = orders.groupBy("customer_id") \
    .agg(F.sum("amount").alias("total")) \
    .filter(F.col("total") > 1000)

customers.join(high_spenders, on="customer_id", how="inner") \
         .select("name", "total") \
         .orderBy(F.col("total").desc())
```

**A10.** Reading bottom-up:
1. `FileScan parquet`: reads only `amount` and `category` columns from Parquet (column pruning). The filter `amount > 500` is pushed into the reader (`PushedFilters`) — only matching rows are read.
2. `Filter`: second filter pass (sometimes Parquet can't fully push all predicates).
3. `Project`: selects only the two needed columns.
4. `HashAggregate (partial)`: partial sum per partition (pre-aggregation before shuffle).
5. `Exchange hashpartitioning`: shuffles data by `category` (necessary for groupBy).
6. `HashAggregate (final)`: combines the partial sums from all partitions into final result.

**A11.** `BroadcastHashJoin`: one side (small table) is broadcast to all executors — NO shuffle. Fast. `SortMergeJoin`: both sides are shuffled by join key, sorted, then merged — expensive network I/O. Whenever you see `SortMergeJoin`, ask: "can I broadcast the smaller side?"

**A12.** (1) **Predicate pushdown**: Catalyst moves the filter before the join, reducing rows that need to be joined. (2) **Broadcast join**: if the small lookup table is < `autoBroadcastJoinThreshold`, Catalyst may automatically broadcast it, eliminating the shuffle entirely.

**A13.** With `shuffle.partitions=200` (default), a groupBy on a small dataset creates 200 partitions after the shuffle — many of them nearly empty, causing scheduling overhead. AQE auto-coalescing merges adjacent small partitions into larger ones after the shuffle, based on actual partition sizes. This is useful for mixed workloads where some operations produce small data and others produce large data.

**A14.** `cache()` is **lazy**. Just calling `cache()` doesn't store anything. The first action (`filter().show()`) triggers computation and stores the result. But wait — `filter().show()` is an action on a **derived** DataFrame, not directly on the cached `df`. The underlying `df` might be getting materialized, but `filter().show()` creates a new DataFrame from `df` and runs it. Let me clarify: if the cache IS being populated on the first action, the second should be fast. The real issue might be that the first action uses a derived DF (`df.filter(...)`) not directly `df`, so `df` may not be the thing getting cached in the plan. Fix: call `df.count()` after `df.cache()` to force materialization.

**A15.** Default: `MEMORY_AND_DISK`. Use `MEMORY_ONLY` when: the dataset fits comfortably in RAM AND you're in an iterative algorithm (e.g., ML training loop) where speed matters most and you'd rather recompute than pay deserialization cost. Risk: if data doesn't fit, partitions are silently dropped and recomputed — no spill to disk.

**A16.** Both materialize a DataFrame. `cache()` stores in memory (with disk fallback), keeps the full lineage (computation graph). `checkpoint()` writes to HDFS/disk AND truncates the lineage — future actions start fresh from the checkpointed files. Use checkpoint when lineage gets too deep (iterative ML, 100+ step pipelines) to avoid StackOverflow and ensure fault tolerance.

**A17.** Yes — memory leak. Each loop iteration caches a new DataFrame but never unpersists the previous one. After N iterations, N DataFrames are consuming memory. Fix:
```python
for date in date_range:
    daily_df = raw.filter(F.col("date") == date).cache()
    daily_df.count()  # materialize
    process(daily_df)
    daily_df.unpersist()  # release memory EACH iteration
```

**A18.** `coalesce(N)` can only **decrease** the partition count. If the current count is already ≤ 100, coalesce(100) is a no-op. Use `repartition(100)` to increase the partition count (it does a full shuffle).

**A19.** When writing with `partitionBy("country")`, Spark creates a separate folder for each unique value: `country=India/`, `country=USA/`, etc. When reading with a filter `WHERE country = 'India'`, Spark's metadata layer sees only the `country=India/` folder needs to be read — all other folders are "pruned" (skipped). For a table with 100 countries, this means reading 1% of the data instead of 100%.

**A20.** 5 million unique user_ids = 5 million folders, each containing tiny files. This "small files problem" causes: (1) massive overhead on the file system metadata (HDFS NameNode or S3 listing); (2) Spark creates one task per file, so millions of tiny tasks; (3) each Parquet file has a header, footer, and metadata overhead that becomes dominant when files are tiny. Never partition on high-cardinality columns.

**A21.** Bucketing: write both tables with `.bucketBy(N, "customer_id").sortBy("customer_id").saveAsTable(...)` using the same N. When Spark detects both sides of a join are bucketed on the same key with the same bucket count, it skips the shuffle entirely — each executor just joins its matching bucket files locally.

**A22.** `coalesce()`. After filtering out 95% of rows, the data is small. `coalesce` merges partitions without a full shuffle — it's cheap. `repartition` would do a full shuffle of the already-reduced data, which is unnecessary overhead. Rule: use coalesce to reduce partitions on already-small data; use repartition to redistribute large or skewed data.

</details>
