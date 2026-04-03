"""
Topic 2: Spark Architecture Demo
=================================
Run this file to see the architecture in action.
Observe:
  - How SparkSession creates a local cluster
  - How data is partitioned across 'virtual executors'
  - The Spark UI URL for visual inspection
  - How tasks map to partitions

Run: python architecture_demo.py
     Then open: http://localhost:4040  ← Spark UI!
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext
import time

# ─────────────────────────────────────────────────────────────
# PART 1: Creating a SparkSession and Inspecting the Cluster
# ─────────────────────────────────────────────────────────────

def create_session_and_inspect():
    """
    Creates a SparkSession in local mode and prints all cluster info.
    local[4] = 4 virtual executors (threads) on your machine.
    """
    print("=" * 60)
    print("PART 1: Spark Session & Cluster Info")
    print("=" * 60)

    spark = SparkSession.builder \
        .appName("Architecture Demo - Week 1") \
        .master("local[4]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "512m") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()

    # Set log level to WARN so our prints are visible
    spark.sparkContext.setLogLevel("WARN")

    sc = spark.sparkContext

    print(f"\n{'─'*40}")
    print(f"Spark Version    : {spark.version}")
    print(f"App Name         : {sc.appName}")
    print(f"Master           : {sc.master}")
    print(f"Default Parallelism: {sc.defaultParallelism}")
    print(f"Spark UI         : http://localhost:4040")
    print(f"{'─'*40}")

    print("""
    Architecture in local[4] mode:
    ┌──────────────────────────────────────┐
    │         Your Machine                 │
    │                                      │
    │  ┌──────────────────────────────┐    │
    │  │        Driver Process         │    │
    │  │   (this Python script)        │    │
    │  │                              │    │
    │  │   SparkContext (DAGScheduler) │    │
    │  │   SparkSession (SQL engine)   │    │
    │  └──────────┬───────────────────┘    │
    │             │ schedules tasks        │
    │  ┌──────────▼───────────────────┐    │
    │  │  4 Virtual Executor Threads   │    │
    │  │  (local[4] = 4 cores)         │    │
    │  │  Thread 1 | Thread 2          │    │
    │  │  Thread 3 | Thread 4          │    │
    │  └──────────────────────────────┘    │
    │  (In a real cluster these would be   │
    │   separate JVMs on separate machines) │
    └──────────────────────────────────────┘
    """)

    return spark


# ─────────────────────────────────────────────────────────────
# PART 2: See Partitions = Tasks in Action
# ─────────────────────────────────────────────────────────────

def see_partitions_and_tasks(spark):
    """
    Demonstrates how data is split into partitions and how each
    partition maps to exactly one Task.

    Partition = the basic unit of parallelism in Spark.
    Task = the computation applied to one partition.
    """
    print("\n" + "=" * 60)
    print("PART 2: Partitions and Tasks")
    print("=" * 60)

    sc = spark.sparkContext

    # Create data with explicit partitions
    data = list(range(1, 101))  # numbers 1 to 100

    # 4 partitions = 4 tasks will run (one per partition)
    rdd_4_parts = sc.parallelize(data, numSlices=4)

    print(f"\nData: numbers 1 to 100")
    print(f"Number of partitions: {rdd_4_parts.getNumPartitions()}")
    print(f"→ Spark will create {rdd_4_parts.getNumPartitions()} Tasks for this stage")
    print(f"→ With local[4], all 4 tasks run in PARALLEL")

    # See what each partition contains
    print(f"\nData distribution across partitions:")
    partitions = rdd_4_parts.glom().collect()  # glom() groups each partition into a list
    for i, partition in enumerate(partitions):
        print(f"  Partition {i}: {len(partition)} elements "
              f"[{partition[0]} ... {partition[-1]}]")

    # Now try with more partitions than cores
    rdd_8_parts = sc.parallelize(data, numSlices=8)
    print(f"\nWith 8 partitions and 4 cores:")
    print(f"  Batch 1: Partitions 0,1,2,3 run in parallel")
    print(f"  Batch 2: Partitions 4,5,6,7 run in parallel")
    print(f"  (Tasks are processed in waves matching core count)")

    # Run a computation and time it
    print(f"\n--- Running a computation (check http://localhost:4040) ---")
    start = time.time()
    result = rdd_4_parts.map(lambda x: x * x).reduce(lambda a, b: a + b)
    elapsed = time.time() - start
    print(f"  Sum of squares 1-100: {result}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  (Go to Spark UI → Jobs → Stages → Tasks to see each partition's task)")


# ─────────────────────────────────────────────────────────────
# PART 3: Driver vs Executor — Who Does What
# ─────────────────────────────────────────────────────────────

def driver_vs_executor_demo(spark):
    """
    Shows what runs on the DRIVER vs what runs on EXECUTORS.

    DRIVER  = the code that plans and collects results
    EXECUTOR = the lambdas/functions inside transformations
    """
    print("\n" + "=" * 60)
    print("PART 3: Driver vs Executor Responsibility")
    print("=" * 60)

    sc = spark.sparkContext

    # This list is created on the DRIVER
    data = ["apache spark", "cluster manager", "worker node", "executor", "task"]
    print(f"\nDriver created this list: {data}")

    # sc.parallelize() → Driver sends data to Executors
    rdd = sc.parallelize(data, 2)
    print(f"Driver sent data to {rdd.getNumPartitions()} partitions (executors)")

    # This lambda runs on EXECUTORS (not on Driver)
    # Think of it as: Driver serializes the function + sends it to executor
    upper_rdd = rdd.map(lambda x: x.upper())
    print(f"\n.map(lambda x: x.upper()) is defined on Driver but runs on Executors")
    print(f"Driver serializes the function → sends to executor → executor applies it")

    # .collect() → Executors send results back to Driver
    result = upper_rdd.collect()
    print(f"\n.collect() brings results back to Driver: {result}")
    print(f"\nWARNING: Never .collect() a huge dataset — it fills Driver RAM!")

    print("""
    Execution flow:
    ┌─────────────────────────────────────────┐
    │              DRIVER                      │
    │  1. Creates data list                    │
    │  2. Calls sc.parallelize() → sends to    │
    │     executors                            │
    │  3. Defines .map(lambda) function        │
    │  4. Serializes & sends function to       │
    │     executors                            │
    │  7. Receives results from .collect()     │
    └──────────────────────────────────────────┘
                    ↕  network
    ┌─────────────────────────────────────────┐
    │            EXECUTORS                     │
    │  5. Receive partitions of data           │
    │  6. Apply the lambda to each element     │
    │  8. Send results back to Driver          │
    └──────────────────────────────────────────┘
    """)


# ─────────────────────────────────────────────────────────────
# PART 4: Spark Configuration — Key Settings
# ─────────────────────────────────────────────────────────────

def show_spark_config(spark):
    """
    Shows important Spark configuration settings.
    These are things you'll tune in production.
    """
    print("\n" + "=" * 60)
    print("PART 4: Key Spark Configuration")
    print("=" * 60)

    # Read current config
    conf = spark.sparkContext.getConf()

    important_configs = [
        "spark.app.name",
        "spark.master",
        "spark.driver.memory",
        "spark.executor.memory",
        "spark.default.parallelism",
        "spark.sql.shuffle.partitions",
    ]

    print("\nCurrent Configuration:")
    for key in important_configs:
        value = conf.get(key, "not set")
        print(f"  {key:<40} = {value}")

    print("""
    Key Config Explanations:
    ─────────────────────────────────────────────────────────
    spark.driver.memory          → RAM for Driver process
                                   (increase if .collect() causes OOM)
    spark.executor.memory        → RAM per Executor
                                   (increase for large datasets)
    spark.executor.cores         → CPU cores per Executor
                                   (more cores = more parallel tasks)
    spark.default.parallelism    → Default number of partitions for RDDs
    spark.sql.shuffle.partitions → Partitions created after a shuffle
                                   (default 200 — often too high for small data,
                                    too low for large data)
    spark.executor.instances     → Number of Executors (on a cluster)
    """)

    print("Common Production Config Pattern:")
    print("""
    spark = SparkSession.builder \\
        .appName("MyETLJob") \\
        .config("spark.executor.memory", "4g") \\
        .config("spark.executor.cores", "4") \\
        .config("spark.executor.instances", "10") \\
        .config("spark.sql.shuffle.partitions", "400") \\
        .config("spark.driver.memory", "2g") \\
        .getOrCreate()

    # Rule of thumb for cluster sizing:
    # Total cores = 10 executors × 4 cores = 40 parallel tasks
    # Total memory = 10 executors × 4 GB = 40 GB executor memory
    # Shuffle partitions ≈ 2-3× total cores = 80-120
    """)


# ─────────────────────────────────────────────────────────────
# PART 5: What happens on the Spark UI
# ─────────────────────────────────────────────────────────────

def trigger_job_for_ui(spark):
    """
    Runs a job and waits so you can explore the Spark UI.
    Go to http://localhost:4040 while this is running.
    """
    print("\n" + "=" * 60)
    print("PART 5: Explore Spark UI at http://localhost:4040")
    print("=" * 60)

    sc = spark.sparkContext

    data = list(range(1, 10001))
    rdd = sc.parallelize(data, numSlices=8)

    print("\nRunning a job... Check http://localhost:4040")
    print("What to look for in the UI:")
    print("  → Jobs tab    : See your job (one per .collect()/.count() etc.)")
    print("  → Stages tab  : See the stages within each job")
    print("  → Tasks tab   : See individual tasks (one per partition)")
    print("  → Executors   : See executor memory and task counts")
    print("  → Storage     : See cached RDDs/DataFrames")
    print("  → Environment : See all configuration settings")

    # Run something meaningful
    result = (
        rdd
        .filter(lambda x: x % 2 == 0)      # keep evens
        .map(lambda x: x * x)               # square them
        .filter(lambda x: x > 1000)         # keep > 1000
        .count()
    )

    print(f"\nResult: {result} even squares > 1000 in range 1-10000")
    print("\nApplication will stay open for 30s — explore the UI!")

    time.sleep(30)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = create_session_and_inspect()
    see_partitions_and_tasks(spark)
    driver_vs_executor_demo(spark)
    show_spark_config(spark)
    trigger_job_for_ui(spark)

    spark.stop()
    print("\nSparkSession stopped. Architecture demo complete.")
    print("Next: Topic 3 — SparkContext vs SparkSession")
