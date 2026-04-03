"""
Topic 5: Lazy Evaluation, Transformations vs Actions, Lineage
=============================================================
Run: python transforms_demo.py
Open http://localhost:4040 to watch Jobs appear only on Actions.

What you'll learn:
  - Lazy evaluation in practice (timing)
  - Every narrow transformation (with examples)
  - Every important action
  - Wide transformation vs narrow in the same job
  - Lineage graph inspection
  - Common action mistakes
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time


def create_spark():
    spark = SparkSession.builder \
        .appName("Week1 - Transformations Demo") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────────────────────
# PART 1: Prove Lazy Evaluation
# ─────────────────────────────────────────────────────────────

def prove_lazy_evaluation(spark):
    """
    Times transformations vs actions to PROVE lazy evaluation.
    Transformations: ~milliseconds (just DAG building)
    Actions: actual computation time
    """
    print("=" * 60)
    print("PART 1: Proving Lazy Evaluation")
    print("=" * 60)

    sc = spark.sparkContext

    # Big data
    big_data = sc.parallelize(range(10_000_000), 4)

    print(f"\nData: 10 million numbers, 4 partitions")
    print(f"\n{'Operation':<40} {'Time':>10}  {'Triggered Execution?'}")
    print("-" * 75)

    # Transformations (lazy)
    ops = [
        ("sc.parallelize(range(10M))",    lambda: big_data),
        (".map(lambda x: x * 2)",          lambda: big_data.map(lambda x: x * 2)),
        (".filter(lambda x: x > 5M)",      lambda: big_data.filter(lambda x: x > 5_000_000)),
        (".map().filter().map()",           lambda: big_data.map(lambda x: x*2).filter(lambda x: x > 0).map(str)),
    ]

    for name, op in ops:
        t0 = time.time()
        result = op()
        elapsed = time.time() - t0
        print(f"  {name:<38} {elapsed*1000:>8.2f}ms  NO (lazy)")

    # Actions (eager)
    mapped = big_data.map(lambda x: x * 2)

    print()
    actions = [
        ("mapped.count()",       lambda: mapped.count()),
        ("mapped.first()",       lambda: mapped.first()),
        ("mapped.take(5)",       lambda: mapped.take(5)),
        ("mapped.sum()",         lambda: mapped.sum()),
    ]

    for name, action in actions:
        t0 = time.time()
        result = action()
        elapsed = time.time() - t0
        print(f"  {name:<38} {elapsed*1000:>8.2f}ms  YES (triggered execution)")

    print(f"\nKey Insight: transformations are instant (build DAG only)")
    print(f"             actions take real time (execute the DAG)")


# ─────────────────────────────────────────────────────────────
# PART 2: Narrow Transformations — All of Them
# ─────────────────────────────────────────────────────────────

def demo_narrow_transformations(spark):
    """
    Shows every important narrow transformation with examples.
    """
    print("\n" + "=" * 60)
    print("PART 2: Narrow Transformations (no shuffle)")
    print("=" * 60)

    # Sample data
    employees = [
        (1, "Alice",  "Engineering", 95000, "India",  ["Python", "Spark"]),
        (2, "Bob",    "Engineering", 88000, "USA",    ["Java", "Scala"]),
        (3, "Carol",  "Marketing",   72000, "India",  ["Excel", "SQL"]),
        (4, "Dave",   "Marketing",   68000, "UK",     ["Python", "R"]),
        (5, "Eve",    "Sales",       55000, "India",  ["Excel"]),
        (6, "Frank",  "Sales",       62000, "USA",    ["SQL", "Python"]),
        (7, None,     "HR",          45000, "India",  ["Excel"]),       # null name
        (8, "Henry",  "Marketing",   78000, None,     ["Python"]),      # null country
    ]

    schema = StructType([
        StructField("id",      IntegerType()),
        StructField("name",    StringType()),
        StructField("dept",    StringType()),
        StructField("salary",  IntegerType()),
        StructField("country", StringType()),
        StructField("skills",  ArrayType(StringType())),
    ])

    df = spark.createDataFrame(employees, schema)

    print("\nOriginal DataFrame:")
    df.show()

    # ── filter() ──────────────────────────────────────────────
    print("\n--- filter() — keep rows matching condition ---")
    df.filter(F.col("salary") > 70000).show()
    # Also valid:
    # df.where(df.salary > 70000)  — same as filter
    # df.filter("salary > 70000") — SQL string condition

    # ── select() ──────────────────────────────────────────────
    print("--- select() — choose columns ---")
    df.select("name", "dept", "salary").show()
    # With expressions:
    df.select(
        F.col("name"),
        F.col("salary"),
        (F.col("salary") * 0.3).alias("tax")
    ).show()

    # ── withColumn() ─────────────────────────────────────────
    print("--- withColumn() — add or replace a column ---")
    df.withColumn("annual_bonus", F.col("salary") * 0.10) \
      .withColumn("salary_grade", F.when(F.col("salary") > 80000, "Senior")
                                   .when(F.col("salary") > 60000, "Mid")
                                   .otherwise("Junior")) \
      .select("name", "salary", "annual_bonus", "salary_grade") \
      .show()

    # ── drop() ────────────────────────────────────────────────
    print("--- drop() — remove columns ---")
    df.drop("skills", "id").show(3)

    # ── withColumnRenamed() ───────────────────────────────────
    print("--- withColumnRenamed() — rename a column ---")
    df.withColumnRenamed("dept", "department").select("name", "department").show(3)

    # ── filter for nulls ──────────────────────────────────────
    print("--- filtering nulls ---")
    df.filter(F.col("name").isNull()).show()
    df.filter(F.col("name").isNotNull()).show(3)
    df.dropna(subset=["name", "country"]).show()  # drop rows where name OR country is null

    # ── fillna() ─────────────────────────────────────────────
    print("--- fillna() — fill nulls ---")
    df.fillna({"name": "Unknown", "country": "N/A"}).show(3)

    # ── limit() ───────────────────────────────────────────────
    print("--- limit() — take first N rows ---")
    df.limit(3).show()

    # ── union() ───────────────────────────────────────────────
    print("--- union() — combine DataFrames ---")
    new_employees = spark.createDataFrame([
        (9, "Isla", "Engineering", 91000, "UK", ["Python"]),
    ], schema)
    df.union(new_employees).count()
    print("  Union resulted in", df.union(new_employees).count(), "rows")

    # ── map() on RDD ──────────────────────────────────────────
    print("\n--- RDD: map() and flatMap() ---")
    sc = spark.sparkContext
    words = sc.parallelize(["hello world", "spark is fast", "learn spark"])

    # map: one output per input
    upper = words.map(lambda s: s.upper())
    print(f"  map(upper): {upper.collect()}")

    # flatMap: flatten output (many outputs per input)
    word_list = words.flatMap(lambda s: s.split())
    print(f"  flatMap(split): {word_list.collect()}")

    # ── mapPartitions() ───────────────────────────────────────
    print("\n--- RDD: mapPartitions() — more efficient for DB writes ---")
    rdd = sc.parallelize(range(12), 3)

    def process_partition(iterator):
        # In production: open DB connection ONCE per partition, not per row
        items = list(iterator)
        print(f"    [Partition] processing {len(items)} items: {items}")
        return iter([sum(items)])  # return sum of each partition

    result = rdd.mapPartitions(process_partition).collect()
    print(f"  partition sums: {result}")


# ─────────────────────────────────────────────────────────────
# PART 3: Wide Transformations — The Shuffle Ones
# ─────────────────────────────────────────────────────────────

def demo_wide_transformations(spark):
    """
    Shows wide transformations and explains why they're expensive.
    """
    print("\n" + "=" * 60)
    print("PART 3: Wide Transformations (shuffle — stage boundary)")
    print("=" * 60)

    employees = [
        (1, "Alice",  "Engineering", 95000, "India"),
        (2, "Bob",    "Engineering", 88000, "USA"),
        (3, "Carol",  "Marketing",   72000, "India"),
        (4, "Dave",   "Marketing",   68000, "UK"),
        (5, "Eve",    "Sales",       55000, "India"),
        (6, "Frank",  "Sales",       62000, "USA"),
        (7, "Grace",  "Engineering", 105000, "India"),
        (8, "Henry",  "Marketing",   78000, "Australia"),
    ]
    df = spark.createDataFrame(employees, ["id","name","dept","salary","country"])

    # ── groupBy + agg ─────────────────────────────────────────
    print("\n--- groupBy().agg() — shuffle! ---")
    df.groupBy("dept").agg(
        F.count("*").alias("headcount"),
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary"),
        F.min("salary").alias("min_salary"),
        F.sum("salary").alias("total_salary")
    ).show()

    # ── orderBy() ────────────────────────────────────────────
    print("--- orderBy() — global sort, shuffle! ---")
    df.orderBy(F.col("salary").desc()).show()

    # ── distinct() ────────────────────────────────────────────
    print("--- distinct() — dedup, shuffle! ---")
    countries = df.select("country").distinct()
    countries.show()
    print(f"  Distinct countries: {countries.count()}")

    # ── dropDuplicates() ─────────────────────────────────────
    print("--- dropDuplicates(subset) — dedup on specific cols ---")
    # This is smarter than distinct() — lets you keep one row per key combination
    df.dropDuplicates(["dept", "country"]).show()

    # ── repartition() ─────────────────────────────────────────
    print("--- repartition() — redistribute partitions (shuffle!) ---")
    print(f"  Before: {df.rdd.getNumPartitions()} partitions")
    repartitioned = df.repartition(8)
    print(f"  After repartition(8): {repartitioned.rdd.getNumPartitions()} partitions")

    repartitioned_by_col = df.repartition(4, "dept")
    print(f"  After repartition(4, 'dept'): {repartitioned_by_col.rdd.getNumPartitions()} partitions")
    print(f"  (All rows with same dept go to same partition — good for joins!)")

    # ── coalesce() — narrow, no shuffle ──────────────────────
    print("\n--- coalesce() — reduce partitions (NO shuffle) ---")
    coalesced = df.coalesce(2)
    print(f"  After coalesce(2): {coalesced.rdd.getNumPartitions()} partitions")
    print(f"  coalesce: no shuffle (just merges adjacent partitions) — use to reduce partitions")
    print(f"  repartition: full shuffle — use to increase OR evenly redistribute")

    # ── RDD Wide: reduceByKey vs groupByKey ──────────────────
    print("\n--- RDD: reduceByKey vs groupByKey ---")
    sc = spark.sparkContext
    pairs = sc.parallelize([
        ("India", 1), ("USA", 1), ("India", 1),
        ("UK", 1), ("USA", 1), ("India", 1),
        ("UK", 1), ("USA", 1)
    ], 3)

    print("\n  reduceByKey (PREFERRED — reduces data before shuffle):")
    # Reduces within each partition FIRST, then shuffles
    result1 = pairs.reduceByKey(lambda a, b: a + b)
    print(f"  {result1.collect()}")

    print("\n  groupByKey (AVOID — shuffles ALL data then reduces):")
    # Shuffles ALL key-value pairs to one partition, THEN groups
    # For large datasets: sends way more data over the network!
    result2 = pairs.groupByKey().mapValues(sum)
    print(f"  {result2.collect()}")

    print("""
    reduceByKey vs groupByKey — WHY reduceByKey is better:
    ─────────────────────────────────────────────────────────
    Data: [("India",1), ("India",1), ("India",1)] on Partition 0

    groupByKey:
      Partition 0 sends: ("India",[1,1,1]) to reducer
      → 3 values shipped over network

    reduceByKey:
      Partition 0 FIRST reduces locally: ("India", 3)
      → Only 1 value shipped over network!

    With millions of records, this difference is massive.
    ─────────────────────────────────────────────────────────
    """)


# ─────────────────────────────────────────────────────────────
# PART 4: All Important Actions
# ─────────────────────────────────────────────────────────────

def demo_actions(spark):
    """
    Demonstrates every important action with when to use each.
    """
    print("\n" + "=" * 60)
    print("PART 4: Actions (trigger execution)")
    print("=" * 60)

    sc = spark.sparkContext
    df = spark.createDataFrame([
        (1, "Alice", 95000),
        (2, "Bob",   88000),
        (3, "Carol", 72000),
        (4, "Dave",  68000),
        (5, "Eve",   55000),
    ], ["id", "name", "salary"])

    rdd = sc.parallelize(range(1, 11), 2)

    print("\n--- count() ---")
    n = df.count()
    print(f"  df.count() = {n}")

    print("\n--- show() ---")
    df.show()               # default: show 20 rows
    df.show(2)              # show 2 rows
    df.show(2, truncate=False)  # don't truncate long strings

    print("\n--- first() ---")
    row = df.first()
    print(f"  df.first() = {row}")
    print(f"  Access fields: row.name = {row.name}, row.salary = {row.salary}")

    print("\n--- take(n) ---")
    rows = df.take(3)
    print(f"  df.take(3) = {rows}")

    print("\n--- collect() — WARNING: only for small data ---")
    all_rows = df.collect()
    print(f"  df.collect() returned {len(all_rows)} rows")
    print(f"  Type: {type(all_rows)}")
    print(f"  To convert to Pandas: df.toPandas()")

    print("\n--- RDD actions ---")
    print(f"  rdd.collect()      = {rdd.collect()}")
    print(f"  rdd.count()        = {rdd.count()}")
    print(f"  rdd.first()        = {rdd.first()}")
    print(f"  rdd.take(3)        = {rdd.take(3)}")
    print(f"  rdd.sum()          = {rdd.sum()}")
    print(f"  rdd.max()          = {rdd.max()}")
    print(f"  rdd.min()          = {rdd.min()}")
    print(f"  rdd.mean()         = {rdd.mean()}")
    print(f"  rdd.reduce(+)      = {rdd.reduce(lambda a,b: a+b)}")

    print("\n--- countByValue() — frequency map (keep small!) ---")
    small_rdd = sc.parallelize(["a","b","a","c","b","a"])
    freq = small_rdd.countByValue()
    print(f"  countByValue() = {dict(freq)}")

    print("\n--- foreach() and foreachPartition() ---")
    # foreach runs a function on each element on the executor side
    # Use for writing to external systems
    results = []
    df.foreach(lambda row: None)  # can't bring results to driver, just side effects
    print("  foreach(): runs function on each row — for side effects (DB writes)")
    print("  foreachPartition(): runs function once per partition — for DB connections")

    print("\n--- write actions ---")
    print("  df.write.mode('overwrite').parquet('/tmp/output/')  → writes Parquet")
    print("  df.write.mode('append').csv('/tmp/output/')         → appends CSV")
    print("  df.write.format('delta').save('/tmp/delta/')        → writes Delta")
    print("  df.write.mode('overwrite').saveAsTable('my_table')  → Hive table")


# ─────────────────────────────────────────────────────────────
# PART 5: Common Mistakes to Avoid
# ─────────────────────────────────────────────────────────────

def demo_common_mistakes(spark):
    """
    Shows patterns that beginners often get wrong.
    """
    print("\n" + "=" * 60)
    print("PART 5: Common Mistakes — Don't Do These!")
    print("=" * 60)

    sc = spark.sparkContext
    rdd = sc.parallelize(range(1000))

    # ── Mistake 1: Calling action in a loop ──────────────────
    print("""
  MISTAKE 1: Calling action inside a loop (triggers many jobs)
  ─────────────────────────────────────────────────────────────
  BAD:
    for threshold in [100, 200, 300]:
        count = rdd.filter(lambda x: x > threshold).count()  # 3 separate jobs!

  GOOD:
    # Compute all thresholds in one pass
    thresholds = [100, 200, 300]
    results = {t: rdd.filter(lambda x: x > t).count() for t in thresholds}
    # Still 3 jobs but at least the intent is clear
    # Better: restructure to avoid multiple counts
    """)

    # ── Mistake 2: collect() on big data ─────────────────────
    print("""
  MISTAKE 2: collect() on large data → Driver OOM
  ─────────────────────────────────────────────────────────────
  BAD:  df.collect()              # loads ALL data to driver RAM!
  GOOD: df.show(20)              # shows sample in terminal
  GOOD: df.take(100)             # safe sample
  GOOD: df.limit(1000).toPandas() # bounded pandas conversion
    """)

    # ── Mistake 3: groupByKey instead of reduceByKey ──────────
    print("""
  MISTAKE 3: groupByKey instead of reduceByKey
  ─────────────────────────────────────────────────────────────
  BAD:  rdd.groupByKey().mapValues(sum)
        → sends ALL values across network first, then sums

  GOOD: rdd.reduceByKey(lambda a, b: a + b)
        → reduces within each partition first (less network traffic)

  Rule: If you're going to aggregate anyway, use reduceByKey/aggregateByKey.
  Only use groupByKey if you truly need the full list of values.
    """)

    # ── Mistake 4: Not caching when reusing data ──────────────
    print("""
  MISTAKE 4: Recomputing the same DataFrame multiple times
  ─────────────────────────────────────────────────────────────
  BAD:
    filtered = df.filter(df.salary > 50000)  # lazy
    count1 = filtered.count()   # Job 1: reads CSV, filters
    count2 = filtered.groupBy("dept").count().show()  # Job 2: reads CSV AGAIN, filters AGAIN!

  GOOD:
    filtered = df.filter(df.salary > 50000).cache()  # mark for caching
    count1 = filtered.count()   # Job 1: reads CSV, filters, CACHES in memory
    count2 = filtered.groupBy("dept").count().show()  # Job 2: reads FROM CACHE!

  Use cache() when you'll use the same DataFrame more than once.
    """)

    # ── Mistake 5: Using Python functions inside transformations ─
    print("""
  MISTAKE 5: Using Python closures that capture large objects
  ─────────────────────────────────────────────────────────────
  BAD:
    big_dict = {... 1 million entries ...}
    rdd.map(lambda x: big_dict.get(x, 0))
    → big_dict is serialized and sent to EACH executor for EACH task!

  GOOD:
    broadcast_dict = sc.broadcast(big_dict)
    rdd.map(lambda x: broadcast_dict.value.get(x, 0))
    → big_dict sent to each executor ONCE and cached there
    """)


# ─────────────────────────────────────────────────────────────
# PART 6: Lineage in Action
# ─────────────────────────────────────────────────────────────

def demo_lineage(spark):
    """Shows lineage graph for RDDs using toDebugString."""
    print("\n" + "=" * 60)
    print("PART 6: Lineage Graph (toDebugString)")
    print("=" * 60)

    sc = spark.sparkContext

    # Build a chain
    rdd_source = sc.parallelize(["hello world", "spark is fast", "learn pyspark"], 2)
    rdd_words   = rdd_source.flatMap(lambda line: line.split())
    rdd_pairs   = rdd_words.map(lambda word: (word, 1))
    rdd_counts  = rdd_pairs.reduceByKey(lambda a, b: a + b)
    rdd_sorted  = rdd_counts.sortByKey()

    print("\nLineage (read bottom-to-top — bottom = oldest ancestor):")
    print(rdd_sorted.toDebugString().decode())

    print("\nMeaning of the indent levels:")
    print("  (1) = 1 parent → narrow dependency (map, filter, flatMap)")
    print("  (N) = N parents → wide dependency (reduceByKey, join)")
    print("  ShuffledRDD → marks a shuffle boundary")

    print("\nFinal word counts:")
    print(rdd_sorted.collect())


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = create_spark()

    prove_lazy_evaluation(spark)
    demo_narrow_transformations(spark)
    demo_wide_transformations(spark)
    demo_actions(spark)
    demo_common_mistakes(spark)
    demo_lineage(spark)

    print("\n" + "=" * 60)
    print("WEEK 1 - TOPIC 5 COMPLETE")
    print("=" * 60)
    print("""
    Summary of Transformations vs Actions:

    TRANSFORMATIONS (lazy — build DAG only):
      Narrow (no shuffle, same stage): filter, select, map, flatMap,
        withColumn, drop, withColumnRenamed, union, limit, coalesce
      Wide (shuffle, new stage): groupBy, join, orderBy, distinct,
        repartition, reduceByKey, sortByKey

    ACTIONS (eager — trigger execution, create Job):
      Return data: collect, take, first, show, head
      Return scalar: count, sum, max, min, mean, reduce
      Side effects: foreach, foreachPartition, saveAsTextFile
      Write: write.parquet, write.csv, write.format(...).save()

    LAZY EVALUATION: Nothing runs until Action is called.
    IMMUTABILITY: Every transformation returns a NEW object.
    LINEAGE: The chain of transformations = fault tolerance plan.
    """)

    spark.stop()
    print("Ready for Week 1 Quiz!")
