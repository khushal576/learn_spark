# Week 7 Quiz — Shuffles, Skew & Spark UI

> Answer without looking at the notebooks. Check answers afterward.

---

## Section 1: Shuffle Internals (10 points)

**Q1.** Describe the two phases of a Sort-Merge shuffle. What happens on disk vs. network in each phase?

**Q2.** You run `orders.explain()` and count 3 Exchange nodes. How many stages will this query create? What is the barrier between each stage?

**Q3.** A job has 100 mapper tasks and `shuffle.partitions=200`. How many shuffle files does it create? What is the formula?

**Q4.** List 4 Spark transformations that trigger a shuffle and 4 that do not.

**Q5.** You have a `groupBy` followed immediately by an `orderBy`. Can Spark combine these into one shuffle? Why or why not?

---

## Section 2: Data Skew (10 points)

**Q6.** In the Spark UI Stage Detail → Tasks, what columns would you look at to confirm that data skew is the cause of a slow stage?

**Q7.** Explain the salting technique step by step for a join. Why must the right side be replicated?

**Q8.** You're salting with `SALT_FACTOR=10`. Customer key "A" originally had 500,000 rows. After salting, approximately how many rows will each "A" sub-partition have?

**Q9.** Why are NULL keys a skew risk in outer joins but not in inner joins?

**Q10.** You have extreme skew: 95% of rows have `user_id=1`. AQE doesn't fully fix it. Write pseudocode for the filter-and-union approach.

---

## Section 3: AQE (8 points)

**Q11.** AQE partition coalescing targets `advisoryPartitionSizeInBytes=64MB`. You have `shuffle.partitions=200` and 100MB of total shuffle data. How many partitions will AQE create? Show your calculation.

**Q12.** Under what conditions will AQE switch a SortMergeJoin to a BroadcastHashJoin at runtime?

**Q13.** AQE has `skewedPartitionFactor=5`. The 8 shuffle partitions after a join have sizes: [10MB, 12MB, 8MB, 250MB, 11MB, 9MB, 10MB, 11MB]. Which partition is detected as skewed? Show the threshold calculation.

**Q14.** Name two situations where AQE cannot help (its limitations).

---

## Section 4: Spark UI (8 points)

**Q15.** You open Stage Detail for a slow stage. Task durations: [45min, 2sec, 1sec, 3sec, 2sec, 1sec, 2sec, 3sec]. What is this pattern called, and what metric would you check next to confirm the cause?

**Q16.** In the SQL tab DAG, you see: `FileScan → Filter → Exchange → HashAggregate`. Is the Filter in the right position? What would be wrong if the Filter appeared AFTER the Exchange?

**Q17.** What does `Spill (Memory) = 3.2GB` and `Spill (Disk) = 2.8GB` mean? What is the relationship between these two numbers?

**Q18.** You see that GC Time = 18 seconds out of a task Duration = 120 seconds. Is this a problem? What's the threshold, and what are two fixes?

---

## Section 5: Optimization Patterns (4 points)

**Q19.** A colleague writes this code. Identify at least 3 problems and write the optimized version.
```python
big_orders = spark.read.csv('/data/orders.csv', header=True, inferSchema=True)
big_products = spark.read.csv('/data/products.csv', header=True, inferSchema=True)
big_customers = spark.read.csv('/data/customers.csv', header=True, inferSchema=True)

result = big_orders.join(big_products, on='product_id') \
                   .join(big_customers, on='customer_id') \
                   .select('*') \
                   .filter(col('category') == 'Electronics') \
                   .filter(col('country') == 'USA') \
                   .groupBy('customer_id').agg(sum('amount')) \
                   .groupBy().agg(avg('amount'))
```

**Q20.** What is `setJobDescription()` and why is it useful in production pipelines?

---

## Answers

<details>
<summary>Click to reveal answers</summary>

**A1.**
```
Shuffle Write (map side — disk):
  1. Each task partitions output by hash(key) % num_partitions
  2. Sorts each partition's data
  3. Writes sorted data to local disk: indexed file + data file
  → CPU (sort) + Disk Write
  → Barrier: all map tasks must finish before any reduce task starts

Shuffle Read (reduce side — network):
  1. Each downstream task fetches its partition's data from ALL upstream tasks
     (over the network from every node that ran map tasks)
  2. Merges the sorted streams (merge-sort)
  3. Processes the merged stream
  → Network I/O + Disk Read (reading the shuffle files) + CPU (merge)
```

**A2.** 4 stages. N Exchange nodes create N+1 stages. Each Exchange is a barrier — all upstream tasks must finish before any downstream task starts.

**A3.** 100 × 200 = 20,000 files. Formula: `M (mappers) × R (reduce partitions)`. Each file is an indexed + data file pair.

**A4.**
Trigger shuffle: `groupBy`, `join (SMJ)`, `distinct`, `repartition`, `orderBy`, `coalesce (sometimes)`, `intersection`, `subtract`
No shuffle: `map/withColumn`, `filter`, `union`, `select/drop`, `limit`, `sample`, `cache`

**A5.** No — they are different operations and typically use different partition keys. `groupBy` shuffles by the group key; `orderBy` shuffles by the sort key (range partition). Catalyst may optimize by combining the HashAggregate + Sort stages in some cases, but they generally remain two separate Exchange nodes unless the keys are identical and Spark can prove range partitioning satisfies groupBy.

**A6.** Check:
- `Duration` column: one task >> all others
- `Shuffle Read Size` (or `Input Size`): the outlier task reads far more data
- `Shuffle Read Records`: confirms the row count difference, not just byte difference

**A7.**
1. Left side: `withColumn("salted_key", concat(col("key"), "_", (rand()*N).cast("int")))`
   → "A" becomes "A_0", "A_1", ... "A_9" — splits 1 partition into N
2. Right side: cross join with range(N) → explode each row into N copies, each with a salt suffix
   → Each customer row becomes N rows: customer_A_0, customer_A_1, ... customer_A_9
3. Join on `salted_key` — now matching keys are spread across N partitions
4. Aggregate on original key (strip salt) to get final results
Right side must be replicated because left-side "A_3" must find a match for "A_3" on the right. Without replication, there's no match for any salted key.

**A8.** ~50,000 rows (500,000 / SALT_FACTOR). Each "A_0" through "A_9" group gets roughly 1/10 of the original "A" rows since the salt is random.

**A9.** In an **inner join**, `NULL != NULL` in SQL — null rows don't match anything, so they're dropped. No one partition accumulates all null rows. In an **outer join**, null rows from the left side are preserved (they don't need a match). Spark hashes null keys — all nulls hash to the same value → same partition → all null rows land on one task.

**A10.**
```python
HOT_KEY = 1
hot = df.filter(col("user_id") == HOT_KEY)
cold = df.filter(col("user_id") != HOT_KEY)

# Process hot separately with more parallelism
hot_result = hot.repartition(20).groupBy("user_id").agg(sum("amount"))

# Normal processing for cold
cold_result = cold.join(dim_table, on="user_id").groupBy("user_id").agg(sum("amount"))

# Union
final = hot_result.union(cold_result)
```

**A11.** 100MB / 64MB ≈ 1.56 → 2 partitions (rounds up to meet the target). AQE groups adjacent partitions: [0..100]→group1 (64MB), [101..200]→group2 (36MB). Result: 2 partitions. `shuffle.partitions=200` was the maximum; AQE reduced it based on actual data.

**A12.** AQE switches SMJ → BHJ at runtime when, after the map stage executes, the actual shuffle write size of one side is smaller than `spark.sql.autoBroadcastJoinThreshold`. This happens when: (a) the table had no statistics at plan time, (b) a filter pushdown reduced the data significantly at runtime, or (c) the plan was built with stale stats.

**A13.**
```
Sizes: [10, 12, 8, 250, 11, 9, 10, 11] MB
Median = (10+11)/2 = 10.5 MB  (sorted: [8,9,10,10,11,11,12,250], median = avg of 4th/5th)
Threshold = 5 × 10.5 = 52.5 MB
Skewed partitions: size > 52.5 MB AND size > skewedPartitionThresholdInBytes (256MB default)

With default 256MB threshold:
  250MB < 256MB → NOT triggered with default thresholds!
  Need to lower skewedPartitionThresholdInBytes to 50MB for this to trigger.

With lowered threshold (50MB):
  Partition index 3 (250MB) > 52.5MB AND > 50MB → SKEWED, AQE splits it
```

**A14.** Two AQE limitations:
1. Streaming queries: AQE requires static plans — not applicable to Structured Streaming
2. First stage: AQE can only re-optimize based on completed shuffles; the very first stage runs with the original plan (no prior shuffle to learn from)
3. (Also valid): AQE can only coalesce (reduce) partitions, not split non-skewed ones to increase parallelism

**A15.** Data skew. One task (45 min) is blocking the stage while all others are <3 seconds. Next metric to check: `Shuffle Read Size` or `Input Size` for that outlier task — it will show it received nearly all the data. Also check which key value maps to that partition.

**A16.** The Filter is before the Exchange — correct! Predicate pushdown is working. If the Filter were AFTER the Exchange, all data would be shuffled across the network first, then filtered. This wastes both network I/O (shuffling rows that will be discarded) and disk I/O (writing them to shuffle files). Filter before Exchange = reduce data BEFORE the expensive operation.

**A17.**
- `Spill (Memory) = 3.2GB`: the data was 3.2GB in deserialized form in memory before it was spilled
- `Spill (Disk) = 2.8GB`: the actual bytes written to disk (smaller because spill files are serialized + compressed, so ~88% of in-memory size in this case)
- Relationship: `Spill (Disk) ≤ Spill (Memory)` always (serialization + compression reduce size). The ratio tells you compression effectiveness.

**A18.** Yes, it's a problem. Rule: GC > 10% of task duration = GC pressure. 18/120 = 15% → alert. Two fixes: (1) Enable G1GC: `spark.executor.extraJavaOptions = -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35`. (2) Use serialized caching: `persist(StorageLevel.MEMORY_ONLY_SER)` to reduce heap objects. Also: reduce executor memory fraction for storage if caching is filling the heap.

**A19.** Problems: (1) `select('*')` — projects all columns when only `customer_id` and `amount` are needed. (2) Two filters on non-join columns before the join — should filter products and customers BEFORE joining to reduce shuffle data. (3) Two groupBys where one will do — `groupBy(customer_id)` then `groupBy()` can be replaced with a single `agg(avg('amount'))` on the original table. (4) No broadcast hints — products and customers are small tables.

Optimized:
```python
orders_slim = spark.read.csv('/data/orders.csv', header=True, inferSchema=True) \
    .select('order_id', 'customer_id', 'product_id', 'amount')

electronics = spark.read.csv('/data/products.csv', header=True, inferSchema=True) \
    .filter(col('category') == 'Electronics') \
    .select('product_id')

usa_customers = spark.read.csv('/data/customers.csv', header=True, inferSchema=True) \
    .filter(col('country') == 'USA') \
    .select('customer_id')

result = orders_slim \
    .join(broadcast(electronics), on='product_id') \
    .join(broadcast(usa_customers), on='customer_id') \
    .agg(avg('amount').alias('avg_amount'))  # single agg, no shuffle
```

**A20.** `spark.sparkContext.setJobDescription("description")` tags subsequent Spark jobs with a human-readable label. In Spark UI → Jobs tab, each job shows this description instead of an auto-generated name. In production pipelines with many steps, this makes it trivial to find which step is slow without counting job IDs or guessing from auto-generated labels. Use it before every logical ETL step.

</details>
