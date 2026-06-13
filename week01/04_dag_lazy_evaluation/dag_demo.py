"""
Topic 4: DAG and Catalyst Optimizer Demo
=========================================
Run: python dag_demo.py
Open http://localhost:4040 → Jobs → DAG Visualization

What you'll learn:
  - How DAG is built incrementally
  - How to read explain() output
  - How Catalyst Optimizer rewrites your plan
  - Predicate pushdown and column pruning in action
  - Lineage graph visualization
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time
import os


# ─────────────────────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────────────────────

def create_spark():
    spark = SparkSession.builder \
        .appName("Week1 - DAG Demo") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_sample_data(spark):
    """Creates sample employee and department data for demos."""
    employees = [
        (1, "Alice",   "Engineering", 95000, "India"),
        (2, "Bob",     "Engineering", 88000, "USA"),
        (3, "Carol",   "Marketing",   72000, "India"),
        (4, "Dave",    "Marketing",   68000, "UK"),
        (5, "Eve",     "Sales",       55000, "India"),
        (6, "Frank",   "Sales",       62000, "USA"),
        (7, "Grace",   "Engineering", 105000, "India"),
        (8, "Henry",   "Marketing",   78000, "Australia"),
        (9, "Isla",    "Engineering", 91000, "UK"),
        (10, "Jack",   "Sales",       59000, "India"),
    ]
    emp_schema = StructType([
        StructField("id",     IntegerType()),
        StructField("name",   StringType()),
        StructField("dept",   StringType()),
        StructField("salary", IntegerType()),
        StructField("country",StringType()),
    ])
    emp_df = spark.createDataFrame(employees, emp_schema)

    departments = [
        ("Engineering", "CTO",   50),
        ("Marketing",   "CMO",   20),
        ("Sales",       "CSO",   30),
    ]
    dept_schema = StructType([
        StructField("dept",     StringType()),
        StructField("head",     StringType()),
        StructField("headcount",IntegerType()),
    ])
    dept_df = spark.createDataFrame(departments, dept_schema)

    return emp_df, dept_df


# ─────────────────────────────────────────────────────────────
# PART 1: Building the DAG Step by Step
# ─────────────────────────────────────────────────────────────

def demo_dag_building(spark, emp_df):
    """
    Shows that transformations build the DAG without executing.
    Uses time.sleep() so you can check Spark UI at each step.
    """
    print("=" * 60)
    print("PART 1: Building the DAG (Nothing Runs Yet)")
    print("=" * 60)

    print("\nStep 1: emp_df already exists (read from memory)")
    print("        DAG: [CreateDataFrame]")
    time.sleep(1)

    print("\nStep 2: Apply filter — NOTHING RUNS")
    filtered = emp_df.filter(F.col("salary") > 70000)
    print("        DAG: [CreateDataFrame] → [Filter: salary > 70000]")
    time.sleep(1)

    print("\nStep 3: Add column — NOTHING RUNS")
    with_tax = filtered.withColumn("tax", F.col("salary") * 0.30)
    print("        DAG: [CreateDataFrame] → [Filter] → [WithColumn: tax]")
    time.sleep(1)

    print("\nStep 4: GroupBy — NOTHING RUNS")
    grouped = with_tax.groupBy("dept").agg(
        F.count("*").alias("headcount"),
        F.avg("salary").alias("avg_salary"),
        F.sum("tax").alias("total_tax")
    )
    print("        DAG: [CreateDataFrame] → [Filter] → [WithColumn] → [GroupBy+Agg]")
    time.sleep(1)

    print("\nStep 5: OrderBy — NOTHING RUNS")
    ordered = grouped.orderBy("avg_salary", ascending=False)
    print("        DAG: [...] → [GroupBy+Agg] → [OrderBy]")

    print("\n\n>>> NOW calling .show() — THIS triggers execution! <<<")
    print("    Watch http://localhost:4040 → Jobs → New Job appears!")
    print()
    ordered.show()

    return filtered, with_tax, grouped, ordered


# ─────────────────────────────────────────────────────────────
# PART 2: Reading explain() — The Text DAG
# ─────────────────────────────────────────────────────────────

def demo_explain(spark, emp_df):
    """
    Shows how to read the explain() output to understand the DAG.
    This is your primary debugging tool in production.
    """
    print("\n" + "=" * 60)
    print("PART 2: Reading explain() Output")
    print("=" * 60)

    query = emp_df.filter(F.col("salary") > 70000) \
                  .groupBy("dept") \
                  .agg(F.avg("salary").alias("avg_salary")) \
                  .orderBy("avg_salary", ascending=False)

    print("\n--- Simple explain() (Physical Plan only) ---")
    print("Read bottom-to-top: first what happens first.\n")
    query.explain()

    print("\n--- explain(True) (All 4 plan stages) ---")
    print("This shows Parsed → Analyzed → Optimized → Physical plans\n")
    query.explain(True)

    print("""
    How to read explain() output:
    ────────────────────────────────────────────────────────────
    Read BOTTOM to TOP — the data flows upward.

    Common operators you'll see:
      FileScan / Scan                → Reading source data
      Filter                         → WHERE clause / .filter()
      Project                        → SELECT columns / .select()
      HashAggregate                  → First pass of groupBy (within partitions)
      Exchange hashpartitioning      → THE SHUFFLE (stage boundary!)
      HashAggregate                  → Final groupBy aggregation
      Sort                           → orderBy
      Exchange rangepartitioning     → Shuffle for global sort
      BroadcastHashJoin              → Join where small table is broadcast
      SortMergeJoin                  → Join where both tables are large

    When you see 'Exchange' → that's a shuffle → stage boundary!
    Each 'Exchange' in the plan = one more stage in execution.
    ────────────────────────────────────────────────────────────
    """)


# ─────────────────────────────────────────────────────────────
# PART 3: Catalyst Optimizer in Action
# ─────────────────────────────────────────────────────────────

def demo_catalyst_optimizer(spark, emp_df, dept_df):
    """
    Shows how Catalyst rewrites your plan for efficiency.
    Compare naive query vs what Catalyst actually runs.
    """
    print("\n" + "=" * 60)
    print("PART 3: Catalyst Optimizer — Predicate Pushdown")
    print("=" * 60)

    # Demo 1: Predicate Pushdown
    print("\n--- Demo: Predicate Pushdown ---")
    print("You write: join first, then filter")
    print("Catalyst rewrites: filter first, then join (faster!)\n")

    naive_query = emp_df \
        .join(dept_df, "dept") \
        .filter(F.col("country") == "India")

    print("Your query:")
    print("  emp_df.join(dept_df, 'dept').filter(country == 'India')")
    print()
    print("What Catalyst actually does (check explain output):")
    naive_query.explain()

    print("""
    Notice in the plan: Filter appears BEFORE or AT the join.
    Catalyst pushed the filter (country == 'India') DOWN closer to the data source.
    This reduces the number of rows that need to be joined.
    """)

    # Demo 2: Column Pruning
    print("\n--- Demo: Column Pruning ---")
    print("You select only 2 columns. Catalyst tells the scan to skip the rest.\n")

    pruned_query = emp_df.select("name", "salary").filter(F.col("salary") > 80000)
    print("Query: emp_df.select('name', 'salary').filter(salary > 80000)")
    print()
    pruned_query.explain()
    print("""
    Notice: The scan shows ReadSchema with only the columns you need.
    Catalyst tells the data source reader: "only read name and salary".
    With Parquet files, this means skipping entire column chunks on disk!
    """)

    # Demo 3: Constant Folding
    print("\n--- Demo: Constant Folding ---")
    print("Spark pre-computes constant expressions at plan time.\n")

    constant_query = emp_df.filter(F.col("salary") > (40000 + 10000))
    print("Query: .filter(salary > (40000 + 10000))")
    print("Catalyst will pre-compute: 40000 + 10000 = 50000 at plan time")
    constant_query.explain()


# ─────────────────────────────────────────────────────────────
# PART 4: DAG as Lineage — Fault Tolerance
# ─────────────────────────────────────────────────────────────

def demo_lineage(spark, emp_df):
    """
    Shows the lineage chain. In a real failure, Spark would
    use this lineage to re-compute lost partitions.
    """
    print("\n" + "=" * 60)
    print("PART 4: Lineage Graph (Fault Tolerance)")
    print("=" * 60)

    # Build a chain of transformations
    rdd1 = spark.sparkContext.parallelize(range(100), 4)
    rdd2 = rdd1.map(lambda x: x * 2)
    rdd3 = rdd2.filter(lambda x: x > 50)
    rdd4 = rdd3.map(lambda x: (x % 10, x))
    rdd5 = rdd4.reduceByKey(lambda a, b: a + b)

    print("\nRDD Lineage Chain:")
    print("  rdd1 = parallelize(range(100))")
    print("  rdd2 = rdd1.map(x * 2)")
    print("  rdd3 = rdd2.filter(x > 50)")
    print("  rdd4 = rdd3.map(x → (x%10, x))")
    print("  rdd5 = rdd4.reduceByKey(a + b)")

    print("\nRDD lineage (toDebugString):")
    print(rdd5.toDebugString().decode())

    print("""
    Lineage for Fault Tolerance:
    ────────────────────────────────────────────────────────
    If executor holding rdd3's partition 2 dies:
      Spark checks: "how was rdd3 partition 2 built?"
      Answer from lineage: rdd1 partition 2 → rdd2 partition 2 → rdd3 partition 2
      Spark re-runs: map(x*2) → filter(x>50) for just partition 2
      Result: rdd3 partition 2 is rebuilt without touching HDFS/S3!

    This is why Spark doesn't need to replicate intermediate data to disk.
    The DAG/lineage IS the recovery plan.
    ────────────────────────────────────────────────────────
    """)

    result = rdd5.collect()
    print(f"Final result (rdd5): {sorted(result)}")


# ─────────────────────────────────────────────────────────────
# PART 5: See the DAG in Spark UI
# ─────────────────────────────────────────────────────────────

def demo_dag_in_ui(spark, emp_df, dept_df):
    """
    Runs a complex query so you can see the full DAG in Spark UI.
    """
    print("\n" + "=" * 60)
    print("PART 5: Complex DAG — View in Spark UI")
    print("=" * 60)

    print("\nRunning a complex query:")
    print("  filter → join → groupBy → window → orderBy")
    print("Go to http://localhost:4040 → Jobs → click job → DAG Visualization")
    print()

    result = emp_df \
        .filter(F.col("salary") > 60000) \
        .join(dept_df, "dept") \
        .groupBy("dept", "head") \
        .agg(
            F.count("*").alias("employees"),
            F.avg("salary").alias("avg_salary"),
            F.max("salary").alias("max_salary")
        ) \
        .orderBy("avg_salary", ascending=False)

    result.show()
    result.explain(True)

    print("\nSpending 30s so you can explore the DAG in Spark UI...")
    time.sleep(30)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = create_spark()
    emp_df, dept_df = create_sample_data(spark)

    demo_dag_building(spark, emp_df)
    demo_explain(spark, emp_df)
    demo_catalyst_optimizer(spark, emp_df, dept_df)
    demo_lineage(spark, emp_df)
    demo_dag_in_ui(spark, emp_df, dept_df)

    print("\n" + "=" * 60)
    print("KEY TAKEAWAYS: DAG")
    print("=" * 60)
    print("""
  1. DAG = the execution plan built from your transformations
  2. Nothing runs until an Action is called (lazy evaluation)
  3. Catalyst Optimizer rewrites the plan for efficiency
     - Predicate Pushdown: filter early
     - Column Pruning: read only needed columns
     - Join Reordering: join smaller tables first
  4. DAG splits at shuffles → Stages
  5. Lineage = fault tolerance without disk replication
  6. explain() = how you inspect the plan in code
  7. Spark UI → DAG Visualization = visual debugging tool
    """)

    spark.stop()
    print("Done. Next: Topic 5 — Transformations vs Actions")
