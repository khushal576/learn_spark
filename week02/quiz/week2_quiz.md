# Week 2 Quiz — Phase 2: RDDs

> Complete all 5 notebooks before attempting. Answers at the bottom.

---

## Section 1: RDD Fundamentals (Notebook 1)

**Q1.** What does RDD stand for? Explain what each word means.

**Q2.** Which of the following is NOT one of the 5 internal properties of an RDD?
- a) A list of partitions
- b) A compute function per partition
- c) A list of column names
- d) Dependencies on parent RDDs (lineage)

**Q3.** You have 8 executor cores and an RDD with 3 partitions. What is the maximum number of tasks that can run in parallel for this RDD?
- a) 8
- b) 3
- c) 24
- d) 1

**Q4.** What does `rdd.glom().collect()` return?
- a) The lineage tree of the RDD
- b) A list of partitions, where each element is a list of items in that partition
- c) The number of partitions
- d) A sample of the RDD data

**Q5 (Short Answer).** Why are RDDs immutable? What would break if they weren't?

---

## Section 2: RDD Transformations (Notebook 2)

**Q6.** What is the output of this code?
```python
rdd = sc.parallelize(["a b", "c d e"])
result = rdd.map(lambda s: s.split()).collect()
```
- a) `["a", "b", "c", "d", "e"]`
- b) `[["a", "b"], ["c", "d", "e"]]`
- c) `[2, 3]`
- d) Error

**Q7.** Which transformation would you use to convert `["hello world", "spark is great"]` into `["hello", "world", "spark", "is", "great"]`?
- a) `map(lambda s: s.split())`
- b) `flatMap(lambda s: s.split())`
- c) `filter(lambda s: " " in s)`
- d) `mapPartitions(lambda s: s.split())`

**Q8.** Is `distinct()` a narrow or wide transformation? Why?
- a) Narrow — it only removes duplicates within each partition
- b) Wide — it must shuffle data so all duplicates end up on the same executor
- c) Narrow — it doesn't change the number of partitions
- d) Wide — it always increases the number of partitions

**Q9.** When should you use `mapPartitions()` instead of `map()`?
- a) When you want to add a new column
- b) When the setup cost (e.g., DB connection) is high and should happen once per partition, not per element
- c) When you want to remove duplicates
- d) When processing key-value pairs

**Q10.** What does `coalesce()` do and how does it differ from `repartition()`?

---

## Section 3: RDD Actions (Notebook 3)

**Q11.** Which action is DANGEROUS to call on a 500GB RDD and why?
- a) `count()` — triggers full scan
- b) `first()` — returns wrong result
- c) `collect()` — loads all data to Driver RAM
- d) `take(10)` — too slow

**Q12.** What does `reduce(lambda a, b: a + b)` compute on `sc.parallelize([1, 2, 3, 4, 5])`?
- a) `[1, 3, 6, 10, 15]`
- b) `15`
- c) `(5, 15)` — (count, sum)
- d) Error — reduce returns None

**Q13.** What is the main advantage of `aggregate()` over `reduce()`?
- a) `aggregate()` is faster
- b) `aggregate()` can return a different type than the input elements
- c) `aggregate()` works on strings; `reduce()` only on numbers
- d) `aggregate()` doesn't require a shuffle

**Q14.** You call `rdd.count()` 3 times in your code. How many Spark Jobs are triggered?
- a) 1
- b) 3 (unless the RDD is cached)
- c) 0 (count is lazy)
- d) Depends on the number of partitions

**Q15 (Short Answer).** What is `foreachPartition()` and when would you use it over `foreach()`?

---

## Section 4: Pair RDDs (Notebook 4)

**Q16.** What is a Pair RDD?
- a) An RDD with exactly 2 partitions
- b) An RDD where every element is a (key, value) 2-tuple
- c) An RDD joined with another RDD
- d) An RDD created from two different data sources

**Q17.** Why is `reduceByKey` preferred over `groupByKey` for aggregation?
- a) `reduceByKey` has a simpler API
- b) `reduceByKey` pre-aggregates within each partition before shuffling, sending less data across the network
- c) `groupByKey` doesn't support Python lambda functions
- d) `reduceByKey` doesn't trigger a shuffle

**Q18.** What does `mapValues(lambda x: x * 2)` do on `[(\"a\", 3), (\"b\", 5)]`?
- a) Returns `[(\"a\", \"a\"), (\"b\", \"b\")]`
- b) Returns `[(\"a\", 6), (\"b\", 10)]`
- c) Returns `[6, 10]`
- d) Returns `[(\"aa\", 3), (\"bb\", 5)]`

**Q19.** You have: `employees = [(1, "Alice"), (2, "Bob"), (3, "Carol")]` and `salaries = [(1, 90000), (2, 75000), (4, 60000)]`. What does `employees.join(salaries).collect()` return?
- a) All 4 employees with None for missing salary
- b) `[(1, ("Alice", 90000)), (2, ("Bob", 75000))]` — only matching IDs
- c) `[(1, ("Alice", 90000)), (2, ("Bob", 75000)), (3, ("Carol", None)), (4, (None, 60000))]`
- d) Error — can't join RDDs

**Q20 (Short Answer).** When would you use `groupByKey` instead of `reduceByKey`? Give a concrete example.

---

## Section 5: Wide vs Narrow & Shuffle (Notebook 5)

**Q21.** Which of the following is a NARROW transformation?
- a) `distinct()`
- b) `sortByKey()`
- c) `flatMap(lambda x: x.split())`
- d) `repartition(8)`

**Q22.** You see this in `toDebugString()` output: two `ShuffledRDD` entries. How many Stages does the job have?
- a) 2
- b) 3
- c) 4
- d) 1

**Q23.** What does data skew mean in Spark and how do you detect it?
- a) When the cluster has unequal machines; detect via cluster manager UI
- b) When one key has far more records than others, causing one task to run much longer; detect in Spark UI → Tasks tab by outlier task durations
- c) When partitions have different data types; detect via schema validation
- d) When too many partitions are created; detect by counting partitions

**Q24.** After calling `rdd.reduceByKey(lambda a,b: a+b)`, what controls the number of output partitions?
- a) `spark.driver.memory`
- b) `spark.sql.shuffle.partitions` (for DataFrames) or the `numPartitions` argument
- c) The number of input partitions
- d) The number of unique keys

**Q25 (Short Answer).** Explain the 3 main steps of a shuffle operation in Spark.

---

## BONUS: Interview Scenario Questions

**B1.** A data engineer says: "I'm processing 50 million user events. I use `groupByKey('user_id').mapValues(len)` to count events per user." What is wrong with this approach and how would you fix it?

**B2.** Your Spark job has this pipeline:
```python
result = data \
    .groupByKey() \
    .mapValues(list) \
    .filter(lambda kv: len(kv[1]) > 100) \
    .map(lambda kv: (kv[0], sum(kv[1]))) \
    .sortByKey()
```
How many shuffles does this have? Can you rewrite it to use fewer shuffles?

**B3.** In production, your Spark job runs fine for 3 months. Then suddenly one job takes 5x longer. Spark UI shows one task taking 45 minutes while all others finish in 2 minutes. What is the likely cause and how would you diagnose and fix it?

---

## ANSWERS

<details>
<summary>Click to reveal answers</summary>

**Q1:** RDD = Resilient (fault-tolerant via lineage recomputation), Distributed (data split across multiple machines/partitions), Dataset (a collection of data records).

**Q2: c** — Column names are a DataFrame concept (schema). RDDs have no schema/column names.

**Q3: b** — Parallelism is limited by BOTH cores AND partitions. With only 3 partitions, only 3 tasks can run at once (one per partition), even though 8 cores are available. The other 5 cores sit idle.

**Q4: b** — `glom()` is a transformation that groups each partition into a list. `collect()` brings all partitions to the Driver, giving you a list of lists where each inner list is one partition's contents.

**Q5:** Immutability enables fault tolerance via lineage. When Spark recomputes a lost partition, it needs to re-run the same transformations on the same parent data to get the same result. If data were mutable, it could have been modified between the original computation and the recomputation — the result would differ, breaking fault tolerance. Immutability guarantees determinism.

**Q6: b** — `map()` applies the function and returns one element per input. Splitting a string returns a list, so the result is a list of lists: `[["a", "b"], ["c", "d", "e"]]`.

**Q7: b** — `flatMap()` applies the function then flattens the result by one level, giving a flat list of words.

**Q8: b** — Wide. Duplicates can exist in any partition. To ensure all copies of the same value are compared, they must be shuffled to the same executor.

**Q9: b** — `mapPartitions` calls your function once per partition with an iterator over all elements. For expensive setup (like DB connections, loading ML models), you pay the cost once per partition rather than once per element.

**Q10:** `coalesce(n)` reduces partition count by merging adjacent partitions — it's a narrow transformation (no shuffle), so it's faster but can produce unbalanced partitions. `repartition(n)` does a full shuffle to redistribute data evenly — it's wide and slower but produces balanced partitions. `coalesce` can only reduce; `repartition` can increase or decrease. Use `coalesce` before writes to reduce output files; use `repartition` when you need more parallelism or even distribution.

**Q11: c** — `collect()` brings ALL data to Driver RAM. 500GB would crash the Driver with OOM. `count()` only returns a Long (fine). `first()` and `take(n)` return small amounts of data (fine).

**Q12: b** — `reduce(+)` sums all elements: 1+2+3+4+5 = 15.

**Q13: b** — `aggregate()` takes a zero value of a different type and two functions, allowing the output to be a different type than the input. `reduce()` requires input and output to be the same type.

**Q14: b** — 3 jobs. Each action triggers a full job. Unless the RDD is cached, Spark recomputes from scratch for each action call.

**Q15:** `foreachPartition(f)` calls `f` once per partition, passing an iterator over all elements. Use it when your function has expensive setup — like opening a database connection. With `foreach`, you'd open the connection for every single element. With `foreachPartition`, you open it once per partition and write all elements in that partition. This dramatically reduces connection overhead for DB writes.

**Q16: b** — A Pair RDD is an RDD of 2-tuples `(key, value)` that enables key-based operations.

**Q17: b** — `reduceByKey` performs a partial aggregation (map-side combine) within each partition before shuffling. Only the partially-reduced values cross the network. `groupByKey` shuffles ALL raw values, then groups them — sending far more data over the network.

**Q18: b** — `mapValues` applies the function to the value only, keeping the key unchanged: `[("a", 6), ("b", 10)]`.

**Q19: b** — `join()` is an inner join — only keys present in BOTH RDDs are included. emp_id 3 (Carol, no salary) and emp_id 4 (no employee, salary 60000) are excluded.

**Q20:** Use `groupByKey` when you need ALL values as a collection to perform a non-associative/non-commutative operation — i.e., one that can't be done incrementally. Example: computing the median. You can't compute `median(a, b)` from two partial medians. You must collect all values per key first, then compute the median. Another example: if you need to find the top-K values per key, keeping track of only a running partial result doesn't work.

**Q21: c** — `flatMap` is narrow (each output partition depends on one input partition). The others are all wide: `distinct` and `sortByKey` shuffle, `repartition` shuffles.

**Q22: b** — 3 stages. Each `ShuffledRDD` = one stage boundary. 2 shuffles create 3 stages: Stage 0 (before first shuffle), Stage 1 (between shuffles), Stage 2 (after second shuffle).

**Q23: b** — Data skew = one key has disproportionately more records. One executor's task takes much longer because it has vastly more data. Detect: Spark UI → Stages → Tasks table → sort by Duration — if one task takes 10x longer than median, that's skew.

**Q24: b** — For RDD `reduceByKey`, pass `numPartitions` as the second argument. For DataFrames, `spark.sql.shuffle.partitions` controls it (default 200).

**Q25:** 3 steps of a shuffle:
1. **Map side**: Each executor processes its partitions, pre-aggregates (for reduceByKey), then writes shuffle files to local disk, organized by target partition (using hash partitioning on the key).
2. **Network transfer**: Reduce-side executors fetch their shuffle data from map-side executors' local disks across the network.
3. **Reduce side**: Each executor receives all data for its assigned keys, performs the final aggregation/sort/group, and produces the output partition.

**B1 — Problem and Fix:**
Problem: `groupByKey` shuffles ALL 50 million events across the network grouped by user_id. If any user has millions of events, that executor OOMs. The subsequent `mapValues(len)` just counts them — which is pure aggregation.
Fix: Use `map(lambda kv: (kv[0], 1)).reduceByKey(lambda a,b: a+b)` — counts events per user with pre-aggregation, shuffling only partial counts (one integer per user per partition) instead of all raw events.

**B2 — Shuffles and rewrite:**
Current pipeline: `groupByKey` (shuffle 1) → filter → map → `sortByKey` (shuffle 2) = 2 shuffles
But the logic is: count groups over 100 and sum their values, sorted by key.

Better rewrite using `aggregateByKey` (1 shuffle instead of 2 before sort):
```python
result = data \
    .aggregateByKey((0, 0), lambda acc, x: (acc[0]+x, acc[1]+1), lambda a,b: (a[0]+b[0], a[1]+b[1])) \
    .filter(lambda kv: kv[1][1] > 100) \
    .mapValues(lambda v: v[0]) \
    .sortByKey()
```
Still 2 shuffles (aggregateByKey + sortByKey), but avoids materializing the full value list into memory on the reducer side.

**B3 — Data skew diagnosis and fix:**
Likely cause: a new "hot key" emerged — e.g., a viral user, a bot flooding events with the same user_id, or a bulk data load for one partition key.

Diagnose:
1. Spark UI → Stages → Tasks → find the outlier task
2. Note the partition number
3. Inspect data: `rdd.mapPartitionsWithIndex(lambda i, it: [(i, sum(1 for _ in it))]).collect()` to see which partition has too much data
4. Add a countByKey (on a sample) to find the hot key

Fix options:
1. Salting: add random suffix to hot key, two-phase reduce
2. Repartition by a better key (include more dimensions)
3. Filter/handle the hot key separately as a special case
4. In DataFrames with AQE enabled: automatic skew join handling (Phase 4)

</details>
