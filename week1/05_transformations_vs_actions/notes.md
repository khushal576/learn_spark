# Topic 5: Lazy Evaluation, Transformations vs Actions, Lineage

> Phase 1 → Week 1 → Topic 5

---

## The Online Food Order Analogy

Imagine ordering food online:

**Lazy Evaluation = the restaurant kitchen:**
- You add items to cart (transformations — no cooking happens)
- You add more items (more transformations — still no cooking)
- You press "Place Order" (action — NOW cooking starts)
- The kitchen doesn't cook each item as you add it to cart
- It waits for the full order, then starts cooking efficiently

Spark does the same — add transformations to your cart, then press "go" with an Action.

---

## 1. Lazy Evaluation

**Lazy evaluation** means Spark does NOT execute transformations immediately when you call them.
Instead, it **records the intent** (adds to the DAG) and waits for an Action.

### Why Lazy?

1. **Optimization**: See the whole plan before executing → Catalyst can reorder, prune, push down
2. **Efficiency**: Group multiple transformations into one scan pass
3. **Avoid unnecessary work**: If you define 5 transformations but only call `.first()`,
   Spark might not need to process all data

```python
# This is LAZY — executes INSTANTLY (just builds DAG, no data read):
df = spark.read.csv("huge_10GB_file.csv")  # no disk read yet
filtered = df.filter(df.age > 25)           # no computation yet
grouped  = filtered.groupBy("city").count() # no computation yet

# This is an ACTION — TRIGGERS actual execution:
grouped.show()  # NOW Spark reads the file, filters, groups, shows
```

### Laziness Can Surprise You

```python
import time

df = spark.read.csv("data.csv")
start = time.time()
df2 = df.filter(df.age > 25)   # returns immediately — lazy
print(time.time() - start)     # prints ~0.001s — no work done!

start = time.time()
df2.count()                    # TRIGGERS execution
print(time.time() - start)     # prints actual computation time
```

---

## 2. Transformations

A **Transformation** takes one DataFrame/RDD and returns a **new** DataFrame/RDD.
They are **lazy** — they build the DAG but don't execute.

Transformations never modify the original data (Spark data is **immutable**).

### 2.1 Narrow Transformations

The data needed to compute each output partition comes from **one** input partition only.
- No data movement across the network
- All happen in the **same Stage**
- Fast and efficient

| Transformation | Description | Example |
|---------------|-------------|---------|
| `filter()` | Keep rows matching condition | `df.filter(df.age > 25)` |
| `map()` | Apply function to each row (RDD) | `rdd.map(lambda x: x * 2)` |
| `flatMap()` | Like map but flatten the output | `rdd.flatMap(lambda x: x.split())` |
| `select()` | Choose columns | `df.select("name", "age")` |
| `withColumn()` | Add/replace a column | `df.withColumn("tax", df.salary * 0.3)` |
| `drop()` | Remove a column | `df.drop("temp_col")` |
| `limit()` | Take first N rows | `df.limit(100)` |
| `sample()` | Random sample | `df.sample(fraction=0.1)` |
| `union()` | Combine two DataFrames | `df1.union(df2)` |
| `mapPartitions()` | Apply function per partition | `rdd.mapPartitions(func)` |

### 2.2 Wide Transformations (Shuffle)

The data needed to compute each output partition comes from **many** input partitions.
Data is **shuffled across the network**. Creates a new Stage.

| Transformation | Description | Example |
|---------------|-------------|---------|
| `groupBy()` | Group rows by key | `df.groupBy("dept")` |
| `agg()` | Aggregate (after groupBy) | `.agg(F.count("*"))` |
| `join()` | Join two DataFrames | `df1.join(df2, "id")` |
| `orderBy()/sort()` | Global sort | `df.orderBy("salary")` |
| `distinct()` | Remove duplicates | `df.distinct()` |
| `repartition()` | Redistribute partitions | `df.repartition(8)` |
| `reduceByKey()` | Reduce values per key (RDD) | `rdd.reduceByKey(lambda a,b: a+b)` |
| `groupByKey()` | Group by key (RDD) | `rdd.groupByKey()` |
| `sortByKey()` | Sort by key (RDD) | `rdd.sortByKey()` |
| `cogroup()` | Co-group two RDDs | `rdd1.cogroup(rdd2)` |

### Narrow vs Wide — Visual

```
NARROW (no shuffle — same stage):
Input Partitions     Output Partitions
  [P0] ──────────────→ [P0]   (filter: keep only rows > 100)
  [P1] ──────────────→ [P1]
  [P2] ──────────────→ [P2]

WIDE (shuffle — new stage):
Input Partitions      (shuffle)    Output Partitions
  [P0] ─────────────┬────────────→ [P0]  (groupBy: "Engineering")
  [P1] ─────────────┼────────────→ [P1]  (groupBy: "Marketing")
  [P2] ─────────────┴────────────→ [P2]  (groupBy: "Sales")
  
  Each output partition collects data from ALL input partitions!
```

---

## 3. Actions

An **Action** is what triggers actual execution. It either:
- Returns data to the Driver (`.collect()`, `.first()`, `.take()`)
- Writes data to storage (`.write.parquet()`)
- Returns a scalar value (`.count()`, `.sum()`, `.max()`)
- Shows data (`.show()`)

One Action = One Job in the Spark UI.

### All Actions You Need to Know

| Action | Returns | When to Use |
|--------|---------|-------------|
| `.collect()` | All rows as Python list | Only on small data! Brings to driver RAM |
| `.show(n)` | Prints first n rows | Interactive debugging |
| `.count()` | Number of rows (Long) | Check data size |
| `.first()` | First row as Row object | Peek at data |
| `.take(n)` | First n rows as list | Peek at small sample |
| `.head(n)` | Same as take(n) | Alias for take |
| `.foreach(func)` | Unit (side effect) | Write to external system |
| `.foreachPartition(func)` | Unit | Efficient writes to DB per partition |
| `.reduce(func)` | Single aggregated value | Custom aggregation (RDD) |
| `.sum()` | Sum | RDD of numbers |
| `.max()` | Max value | RDD of numbers |
| `.min()` | Min value | RDD of numbers |
| `.mean()` | Average | RDD of numbers |
| `.countByValue()` | Dict of counts | Frequency count (small cardinality) |
| `.saveAsTextFile()` | None | Write RDD to text (legacy) |
| `df.write.parquet()` | None | Write DataFrame to Parquet |
| `df.write.csv()` | None | Write DataFrame to CSV |
| `df.write.format("delta").save()` | None | Write to Delta Lake |

### Actions That Are Traps

```python
# DANGEROUS: collect() on huge data fills Driver RAM → OOM!
huge_df.collect()  # ❌ Never do this on 10M+ rows

# SAFE alternatives:
huge_df.take(100)           # ✅ get a sample
huge_df.limit(100).collect() # ✅ also safe (Spark pushes limit to execution)
huge_df.show(20)            # ✅ shows 20 rows in terminal

# EXPENSIVE: count() triggers a full scan!
# Don't call count() repeatedly in a loop
for table in tables:
    print(df.count())  # ❌ triggers N jobs

# BETTER: compute once
count = df.count()         # ✅ one job
```

---

## 4. The Transformation-Action Pattern

The standard pattern in every Spark job:

```python
# 1. Read (lazy)
df = spark.read.parquet("s3://bucket/raw/")

# 2. Transform (lazy — all just building DAG)
cleaned = df.filter(F.col("amount") > 0) \
             .dropDuplicates(["transaction_id"]) \
             .withColumn("amount_usd", F.col("amount") / F.col("exchange_rate")) \
             .filter(F.col("country").isNotNull())

enriched = cleaned.join(country_df, "country_code") \
                   .withColumn("region", F.col("region").upper())

aggregated = enriched.groupBy("region", "product_category") \
                      .agg(
                          F.sum("amount_usd").alias("total_revenue"),
                          F.count("*").alias("transaction_count"),
                          F.avg("amount_usd").alias("avg_order_value")
                      )

# 3. Action (triggers execution!)
aggregated.write.mode("overwrite").partitionBy("region").parquet("s3://bucket/gold/")
```

Everything from step 1 to the write builds the DAG. The write triggers execution.

---

## 5. Immutability and Lineage

Spark DataFrames and RDDs are **immutable** — you can never change them in place.
Every transformation returns a NEW object.

```python
df = spark.read.csv("data.csv")
df_filtered = df.filter(df.age > 25)   # new DataFrame, df unchanged
df_renamed = df_filtered.withColumnRenamed("age", "years")  # another new DataFrame

# df, df_filtered, df_renamed are all separate immutable objects
# Each one has a lineage back to df (and ultimately to the CSV file)
```

This immutability is what makes lineage and fault tolerance possible.

---

## 6. Lineage Graph

The **lineage graph** is the chain of transformations from the original data
source to the current RDD/DataFrame. It IS the DAG.

```python
rdd1 = sc.textFile("logs.txt")            # Level 0: source
rdd2 = rdd1.flatMap(lambda l: l.split()) # Level 1: flatMap
rdd3 = rdd2.filter(lambda w: len(w) > 3) # Level 2: filter
rdd4 = rdd3.map(lambda w: (w, 1))        # Level 3: map
rdd5 = rdd4.reduceByKey(lambda a,b: a+b) # Level 4: reduceByKey (shuffle!)

# The lineage:
# logs.txt → flatMap → filter → map → [SHUFFLE] → reduceByKey
```

If executor holding rdd3's partition 1 crashes:
- Spark looks up the lineage: "rdd3 came from rdd2 which came from rdd1 (logs.txt)"
- Spark re-reads partition 1 of logs.txt → applies flatMap → applies filter
- rdd3 partition 1 is rebuilt — no extra disk needed

### When Lineage Becomes Too Long

For very long lineage chains (many transformation steps), re-computation on failure
can be expensive. Solution: **checkpoint()** — breaks the lineage by saving to disk.

```python
# If lineage is 50+ steps deep, checkpoint to reset it
sc.setCheckpointDir("/tmp/spark-checkpoints")
rdd5.checkpoint()
rdd5.count()  # forces checkpoint materialization
# Now rdd5's lineage starts fresh from the checkpoint file
```

---

## 7. Quick Reference

### Is it Lazy or Eager?

| Operation | Type | Lazy/Eager |
|-----------|------|------------|
| `spark.read.csv()` | "transformation" | Lazy |
| `df.filter()` | Transformation | Lazy |
| `df.select()` | Transformation | Lazy |
| `df.withColumn()` | Transformation | Lazy |
| `df.groupBy()` | Transformation | Lazy |
| `df.join()` | Transformation | Lazy |
| `df.count()` | Action | **Eager** |
| `df.show()` | Action | **Eager** |
| `df.collect()` | Action | **Eager** |
| `df.write.parquet()` | Action | **Eager** |
| `df.cache()` | Transformation (sort of) | Lazy |
| `df.persist()` | Transformation (sort of) | Lazy |

Note: `cache()` and `persist()` are lazy — they mark the DF for caching but
don't actually cache until an Action triggers execution.

---

## 8. Interview Cheat Sheet

**Q: What is lazy evaluation in Spark?**
> Lazy evaluation means transformations are not executed immediately when called.
> Instead, Spark records them in the DAG (logical plan). Execution only happens when
> an Action is called (count, show, collect, write). This allows Spark to see the
> complete plan and optimize it before running.

**Q: What is the difference between a transformation and an action?**
> A transformation takes a DataFrame/RDD and returns a new one — it is lazy and
> just builds the DAG. An action triggers actual computation by executing the DAG.
> Actions either return data to the driver (collect, count, show) or write to storage
> (write.parquet). One action = one Spark Job.

**Q: What is the difference between narrow and wide transformations?**
> Narrow: each output partition depends on one input partition only. No shuffle.
> Stays in the same Stage. Examples: filter, map, select, withColumn.
> Wide: each output partition can depend on multiple input partitions. Requires a
> shuffle (data redistribution across network). Creates a new Stage. Examples:
> groupBy, join, orderBy, distinct.

**Q: What is the lineage in Spark?**
> Lineage is the complete chain of transformations from the original data source to the
> current RDD/DataFrame. Spark stores this as the DAG. Lineage enables fault tolerance:
> if a partition is lost, Spark re-runs the lineage chain for just that partition
> instead of requiring replicated intermediate data on disk.

**Q: Why are Spark DataFrames immutable?**
> Immutability enables lineage-based fault tolerance and allows multiple programs to
> share data without interference. Every transformation returns a new DataFrame — the
> original is never modified. This makes the lineage graph reliable: the graph always
> accurately represents how to reconstruct any piece of data.

**Q: Can you give an example where lazy evaluation helps performance?**
> Yes — if you write `df.filter(country == 'India').first()`, Spark doesn't need to
> scan and filter ALL data to get the first matching row. Because of laziness, Spark's
> optimizer can push the filter to the data source and stop as soon as one matching row
> is found — potentially reading just one partition instead of the entire dataset.
