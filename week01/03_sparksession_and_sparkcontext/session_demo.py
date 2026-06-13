"""
Topic 3: SparkSession, SparkContext, Jobs, Stages, Tasks Demo
=============================================================
Run: python session_demo.py
Then open http://localhost:4040 to see Jobs and Stages visually.

What you'll learn:
  - How to create and configure SparkSession
  - SparkContext access and usage
  - How actions trigger Jobs
  - How shuffles create Stage boundaries
  - How to read the Spark UI Jobs/Stages/Tasks tabs
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time


# ─────────────────────────────────────────────────────────────
# PART 1: SparkSession Creation Patterns
# ─────────────────────────────────────────────────────────────

def demo_spark_session_creation():
    """
    Shows different ways to create and configure SparkSession.
    """
    print("=" * 60)
    print("PART 1: SparkSession Creation")
    print("=" * 60)

    # ── Standard way ──────────────────────────────────────────
    spark = SparkSession.builder \
        .appName("Week1 - Session Demo") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"\nSparkSession created:")
    print(f"  spark.version              = {spark.version}")
    print(f"  spark.sparkContext.master  = {spark.sparkContext.master}")
    print(f"  spark.sparkContext.appName = {spark.sparkContext.appName}")

    # ── getOrCreate returns same session ──────────────────────
    spark2 = SparkSession.builder.getOrCreate()
    print(f"\nCalling getOrCreate() again returns same session: {spark is spark2}")

    # ── Access SparkContext ────────────────────────────────────
    sc = spark.sparkContext
    print(f"\nSparkContext from session: {sc}")
    print(f"  sc.defaultParallelism     = {sc.defaultParallelism}")
    print(f"  sc.defaultMinPartitions   = {sc.defaultMinPartitions}")

    # ── Runtime configuration ──────────────────────────────────
    # You can change SQL configs at runtime (not all configs can change)
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    print(f"\n  spark.sql.shuffle.partitions changed to: "
          f"{spark.conf.get('spark.sql.shuffle.partitions')}")

    return spark


# ─────────────────────────────────────────────────────────────
# PART 2: Actions Trigger Jobs — Watch the Spark UI
# ─────────────────────────────────────────────────────────────

def demo_jobs_from_actions(spark):
    """
    Demonstrates that each action triggers a separate Job.
    After running, look at Spark UI → Jobs tab to see 4 jobs.
    """
    print("\n" + "=" * 60)
    print("PART 2: Actions Create Jobs")
    print("=" * 60)

    sc = spark.sparkContext
    data = list(range(1, 1001))
    rdd = sc.parallelize(data, 4)

    print("\nWe have 1 RDD. Watch: each action = 1 new Job in Spark UI.")
    print("Go to http://localhost:4040 → Jobs")

    # Action 1 → Job 1
    print("\n--- Action 1: count() ---")
    count = rdd.count()
    print(f"  count = {count}  → Check Spark UI: Job 0 appeared")
    time.sleep(2)

    # Action 2 → Job 2
    print("\n--- Action 2: first() ---")
    first = rdd.first()
    print(f"  first = {first}  → Check Spark UI: Job 1 appeared")
    time.sleep(2)

    # Action 3 → Job 3
    print("\n--- Action 3: sum() ---")
    total = rdd.sum()
    print(f"  sum = {total}  → Check Spark UI: Job 2 appeared")
    time.sleep(2)

    # Action 4 → Job 4
    print("\n--- Action 4: collect() ---")
    result = rdd.take(5)
    print(f"  take(5) = {result}  → Check Spark UI: Job 3 appeared")

    print("\nTotal: 4 actions = 4 Jobs")


# ─────────────────────────────────────────────────────────────
# PART 3: Stages — Created at Shuffle Boundaries
# ─────────────────────────────────────────────────────────────

def demo_stages_from_shuffles(spark):
    """
    Shows how shuffles create stage boundaries.
    One groupBy = one extra stage.
    Watch the Stages tab in Spark UI.
    """
    print("\n" + "=" * 60)
    print("PART 3: Stages from Shuffles")
    print("=" * 60)

    # Sample employee data
    employees = [
        ("Alice", "Engineering", 95000),
        ("Bob", "Engineering", 88000),
        ("Carol", "Marketing", 72000),
        ("Dave", "Marketing", 68000),
        ("Eve", "Sales", 55000),
        ("Frank", "Sales", 62000),
        ("Grace", "Engineering", 105000),
        ("Henry", "Marketing", 78000),
    ]

    df = spark.createDataFrame(employees, ["name", "dept", "salary"])

    print("\nEmployee DataFrame created (4 partitions):")
    print("  Transformations so far: NONE (lazy)")

    # --- Job with NO shuffle → 1 stage ---
    print("\n--- Query 1: filter only (no shuffle) ---")
    print("  df.filter(salary > 70000).count()")
    print("  Expected: 1 Stage (no groupBy, no shuffle)")

    start = time.time()
    count1 = df.filter(F.col("salary") > 70000).count()
    time1 = time.time() - start

    print(f"  Result: {count1} employees earning > 70000")
    print(f"  Time: {time1:.4f}s")
    print(f"  → Spark UI: 1 Stage, filter ran within each partition")
    time.sleep(2)

    # --- Job WITH shuffle → 2 stages ---
    print("\n--- Query 2: groupBy + count (shuffle!) ---")
    print("  df.groupBy('dept').count().show()")
    print("  Expected: 2 Stages (shuffle at groupBy boundary)")

    start = time.time()
    df.groupBy("dept").count().show()
    time2 = time.time() - start

    print(f"  Time: {time2:.4f}s")
    print(f"  → Spark UI: 2 Stages!")
    print(f"     Stage 0: Read + partial aggregation (pre-shuffle)")
    print(f"     Stage 1: Final aggregation (post-shuffle)")
    time.sleep(2)

    # --- Job with multiple shuffles → 3 stages ---
    print("\n--- Query 3: groupBy + orderBy (2 shuffles!) ---")
    print("  df.groupBy('dept').avg('salary').orderBy('avg(salary)').show()")
    print("  Expected: 3 Stages (groupBy shuffle + orderBy shuffle)")

    start = time.time()
    df.groupBy("dept") \
      .agg(F.avg("salary").alias("avg_salary")) \
      .orderBy("avg_salary", ascending=False) \
      .show()
    time3 = time.time() - start

    print(f"  Time: {time3:.4f}s")
    print(f"  → Spark UI: 3 Stages!")
    print(f"     Stage 0: Read (narrow)")
    print(f"     Stage 1: GroupBy aggregation (after 1st shuffle)")
    print(f"     Stage 2: Sort (after 2nd shuffle)")

    print("""
    Stage Boundary Rule:
    ────────────────────────────────────────────────────────
    Narrow transformation  (filter, map, select, withColumn)
        → SAME Stage, no shuffle
    Wide transformation    (groupBy, join, orderBy, distinct)
        → NEW Stage, data shuffled across network
    ────────────────────────────────────────────────────────
    More shuffles = more network I/O = slower job
    Optimization goal: MINIMIZE shuffles
    """)


# ─────────────────────────────────────────────────────────────
# PART 4: Tasks — One Per Partition
# ─────────────────────────────────────────────────────────────

def demo_tasks_per_partition(spark):
    """
    Demonstrates the 1:1 relationship between partitions and tasks.
    """
    print("\n" + "=" * 60)
    print("PART 4: Tasks = Partitions")
    print("=" * 60)

    sc = spark.sparkContext

    for num_partitions in [2, 4, 8]:
        rdd = sc.parallelize(range(1000), numSlices=num_partitions)
        result = rdd.map(lambda x: x * 2).sum()
        print(f"\n  Partitions: {num_partitions:2d} → "
              f"Tasks in stage: {num_partitions:2d} → "
              f"Sum (×2): {result}")

    print("""
    Partition → Task Relationship:
    ─────────────────────────────────────────────────────
    • 1 partition = 1 task = 1 executor core slot
    • More partitions = more parallelism (up to executor core count)
    • Too few partitions = underuse of executors (cores idle)
    • Too many partitions = overhead (task scheduling cost)
    • Rule of thumb: 2-4× the number of executor cores
    ─────────────────────────────────────────────────────

    With local[4] (4 cores):
      4  partitions → 4 tasks, all run at once    (ideal)
      8  partitions → 2 waves of 4 tasks          (fine)
      2  partitions → 2 tasks, 2 cores idle       (underutilized)
      100 partitions → 25 waves of 4 tasks        (overhead)
    """)


# ─────────────────────────────────────────────────────────────
# PART 5: SparkContext Direct RDD Operations
# ─────────────────────────────────────────────────────────────

def demo_sparkcontext_operations(spark):
    """
    Shows operations that are only available via SparkContext (sc),
    not SparkSession. These are the low-level RDD operations.
    """
    print("\n" + "=" * 60)
    print("PART 5: SparkContext-Specific Operations")
    print("=" * 60)

    sc = spark.sparkContext

    # 1. parallelize — create RDD from local collection
    rdd1 = sc.parallelize([1, 2, 3, 4, 5], 2)
    print(f"\nsc.parallelize([1-5], 2 partitions): {rdd1.collect()}")

    # 2. textFile — read a text file as RDD of lines
    # (We'll use a Python string simulation here)
    # In real usage: sc.textFile("hdfs://path/to/file.txt")
    print(f"\nsc.textFile('path') → creates 1 RDD element per line")
    print(f"  Common for reading raw logs in RDD-style")

    # 3. broadcast — send read-only data to all executors
    lookup = {"Engineering": "Tech", "Marketing": "Business", "Sales": "Revenue"}
    broadcast_lookup = sc.broadcast(lookup)
    print(f"\nsc.broadcast(dict) → sends dict to ALL executors")
    print(f"  broadcast_lookup.value = {broadcast_lookup.value}")
    print(f"  Use for: join a large RDD with a small lookup table")
    print(f"  (More detail in Phase 4: Performance & Optimization)")

    # 4. accumulator — distributed counter/sum
    counter = sc.accumulator(0)
    rdd = sc.parallelize(range(100), 4)
    rdd.foreach(lambda x: counter.add(1))
    print(f"\nsc.accumulator(0) → distributed counter")
    print(f"  After processing 100 elements: counter = {counter.value}")
    print(f"  Use for: counting records, tracking errors in pipelines")

    broadcast_lookup.unpersist()  # clean up


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = demo_spark_session_creation()

    demo_jobs_from_actions(spark)
    demo_stages_from_shuffles(spark)
    demo_tasks_per_partition(spark)
    demo_sparkcontext_operations(spark)

    print("\n" + "=" * 60)
    print("SUMMARY: Jobs → Stages → Tasks")
    print("=" * 60)
    print("""
    APPLICATION
    └── JOB (one per Action: .count(), .show(), .collect(), .write())
        └── STAGE (split at each shuffle: groupBy, join, orderBy)
            └── TASK (one per partition, runs on one executor core)

    SparkSession  = modern entry point (DataFrame + SQL + Streaming)
    SparkContext  = underlying connection to cluster (RDDs, broadcast, accumulator)
    getOrCreate() = returns existing session or creates new one
    """)

    print("Leaving Spark UI open for 30s — explore http://localhost:4040")
    time.sleep(30)

    spark.stop()
    print("Done. Next: Topic 4 — DAG and Lazy Evaluation")
