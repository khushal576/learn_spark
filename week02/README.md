# Week 2 — Phase 2: RDDs (Resilient Distributed Datasets)

> **Phase**: 2 of 7 | **Week**: 2 | **Format**: Jupyter Notebooks (`.ipynb`)
>
> Prerequisites: Week 1 complete (Phase 1 Foundation)

---

## Notebooks This Week

| # | Notebook | Topics Covered |
|---|----------|----------------|
| 1 | [01_rdd_fundamentals.ipynb](01_rdd_fundamentals.ipynb) | What is an RDD, 5 properties, 3 creation methods, partitions, immutability, lineage |
| 2 | [02_rdd_transformations.ipynb](02_rdd_transformations.ipynb) | map, flatMap, filter, distinct, union, mapPartitions, coalesce, sample |
| 3 | [03_rdd_actions.ipynb](03_rdd_actions.ipynb) | collect, count, first, take, reduce, fold, aggregate, foreach, saveAsTextFile |
| 4 | [04_pair_rdds.ipynb](04_pair_rdds.ipynb) | reduceByKey, groupByKey, sortByKey, mapValues, join, aggregateByKey |
| 5 | [05_wide_vs_narrow_shuffle.ipynb](05_wide_vs_narrow_shuffle.ipynb) | Narrow vs wide, shuffle internals, data skew, salting, repartition vs coalesce |

Each notebook has:
- **Notes** (concept + analogy + when to use) as Markdown cells
- **Code** (runnable examples) as Code cells
- **Interview Q&A** inside the notebook
- **Exercises** at the end

---

## How to Run

```bash
# Option A: Jupyter (recommended — see results inline)
pip install -r requirements.txt
jupyter notebook week2/

# Option B: Docker
docker-compose up -d
# Open http://localhost:8888 → navigate to week2/
```

---

## RDD Quick Reference

### Creation
```python
sc.parallelize([1, 2, 3], numSlices=4)      # from list
sc.parallelize(range(100), 8)               # from range
sc.textFile("path/to/file.txt")             # from file (1 line = 1 element)
rdd2 = rdd.map(lambda x: x * 2)            # from transformation
```

### Inspect
```python
rdd.getNumPartitions()                      # partition count
rdd.glom().collect()                        # see each partition's contents
rdd.toDebugString().decode()                # lineage graph (read bottom-to-top)
```

### Narrow Transformations (no shuffle)
```python
rdd.map(lambda x: x * 2)                   # 1 → 1
rdd.flatMap(lambda x: x.split())           # 1 → N (flattened)
rdd.filter(lambda x: x > 0)               # 1 → 0 or 1
rdd.mapValues(lambda v: v + 1)             # pair RDD: apply to values only
rdd.union(rdd2)                            # combine (no dedup)
rdd.coalesce(2)                            # reduce partitions (no shuffle)
rdd.sample(False, 0.1, seed=42)            # random sample
rdd.mapPartitions(func)                    # once per partition (for DB writes)
```

### Wide Transformations (shuffle = stage boundary)
```python
rdd.distinct()                             # remove duplicates
rdd.repartition(8)                         # redistribute (balanced)
rdd.sortBy(lambda x: x, ascending=False)  # global sort
rdd.reduceByKey(lambda a, b: a + b)       # aggregate per key (PREFERRED)
rdd.groupByKey()                           # group all values per key (careful!)
rdd.aggregateByKey(zero, seqOp, combOp)   # aggregate with type change
rdd.sortByKey()                            # sort by key
rdd.join(rdd2)                             # inner join on key
rdd.leftOuterJoin(rdd2)                    # left join
```

### Actions (trigger execution)
```python
rdd.collect()           # ALL rows → Driver (⚠️ OOM risk on large data)
rdd.count()             # number of elements
rdd.first()             # first element
rdd.take(n)             # first n elements
rdd.takeSample(f,n,s)   # n random elements
rdd.takeOrdered(n)      # n smallest elements
rdd.top(n)              # n largest elements
rdd.reduce(f)           # fold all elements into one
rdd.fold(zero, f)       # reduce with identity value (safe on empty RDD)
rdd.aggregate(z,s,c)    # reduce with type change (most powerful)
rdd.sum()               # sum (numeric RDD)
rdd.max() / .min()      # max/min
rdd.mean() / .stdev()   # mean/standard deviation
rdd.stats()             # count + mean + stdev + max + min in one pass
rdd.countByValue()      # frequency dict (small cardinality only)
rdd.foreach(f)          # side effect on each element (DB write, etc.)
rdd.foreachPartition(f) # side effect per partition (open conn once)
rdd.saveAsTextFile(p)   # write to text files
```

---

## Key Rules to Remember

### The #1 Interview Rule: reduceByKey > groupByKey
```
groupByKey:  ALL values → shuffle → group → aggregate
reduceByKey: partial aggregate → shuffle (less data!) → final aggregate
```

### Narrow vs Wide
```
Narrow:  filter, map, flatMap, union, coalesce → same stage, fast
Wide:    groupByKey, reduceByKey, join, distinct, repartition, sort → new stage, shuffle
```

### Action Safety
```
SAFE on large data:  count(), first(), take(n), stats()
DANGEROUS on large data: collect()  ← use take() or show() instead
```

---

## Interview Questions — Week 2

1. What does RDD stand for? What are its 5 properties?
2. What is the difference between `map()` and `flatMap()`?
3. Is `distinct()` narrow or wide? Why?
4. What is a Pair RDD?
5. Why is `reduceByKey` preferred over `groupByKey`?
6. When would you use `groupByKey` over `reduceByKey`?
7. What is a shuffle and why is it expensive?
8. What is data skew? How do you detect and fix it?
9. What is the difference between `repartition()` and `coalesce()`?
10. What does `aggregate()` do that `reduce()` cannot?
11. Why is `collect()` dangerous on large datasets?
12. What does `mapPartitions()` do and when would you use it?
13. What is `toDebugString()` used for?
14. How many stages does a job with 2 shuffles have?

---

## Next Week

**Week 3: Phase 3 — DataFrames & Spark SQL (Part 1)**
- DataFrame vs RDD — why DataFrames are faster (Catalyst + Tungsten)
- Schema: StructType, StructField, inferSchema vs explicit schema
- Reading CSV, JSON, Parquet, ORC
- `select`, `filter`, `withColumn`, `groupBy`, `agg`
- All aggregation functions: count, sum, avg, min, max, collect_list, collect_set
