"""
Topic 1: MapReduce Simulation in Pure Python
============================================
This file simulates what Hadoop MapReduce does — so you can SEE
the disk I/O penalty visually. No Spark yet. Just the concept.

Run it with:  python mapreduce_simulation.py

What you will learn:
  - The Map → Shuffle → Reduce flow
  - Why disk writes between steps are expensive
  - Why iterative jobs compound this cost
"""

import time
import json
import os
import tempfile
from collections import defaultdict

# ─────────────────────────────────────────────────────────────
# PART 1: Simulate MapReduce Word Count with "Fake Disk Writes"
# ─────────────────────────────────────────────────────────────

SAMPLE_DATA = [
    "apache spark is a unified analytics engine for large scale data processing",
    "spark provides high level apis in java scala python and r",
    "spark also supports a rich set of higher level tools",
    "spark sql for sql and structured data processing",
    "mllib for machine learning spark streaming and graphx for graph processing",
    "apache hadoop uses mapreduce which writes to disk between every step",
    "spark keeps data in memory making it much faster than hadoop mapreduce",
    "data engineering involves building pipelines that process large scale data",
    "apache spark apache hadoop apache kafka are popular in data engineering",
]


def simulate_map_phase(data: list[str], job_id: int) -> list[tuple]:
    """
    MAP PHASE: Each 'worker' reads its chunk and emits (key, value) pairs.
    In real MapReduce: each DataNode processes its local HDFS blocks.
    """
    print(f"\n[Job {job_id}] --- MAP PHASE ---")
    print(f"  Input: {len(data)} lines")

    key_value_pairs = []
    for line in data:
        words = line.lower().split()
        for word in words:
            key_value_pairs.append((word, 1))

    print(f"  Output: {len(key_value_pairs)} (word, 1) pairs")
    return key_value_pairs


def simulate_shuffle_sort(key_value_pairs: list[tuple], job_id: int) -> dict:
    """
    SHUFFLE & SORT PHASE: All pairs with the same key go to the same reducer.
    In real MapReduce: this causes NETWORK I/O across the cluster.
    This is called a 'shuffle' — it's expensive and Spark tries to minimize it.
    """
    print(f"\n[Job {job_id}] --- SHUFFLE & SORT PHASE ---")
    print(f"  Grouping {len(key_value_pairs)} pairs by key...")
    print(f"  (In real MapReduce: this data crosses the network!)")

    grouped = defaultdict(list)
    for key, value in key_value_pairs:
        grouped[key].append(value)

    print(f"  Grouped into {len(grouped)} unique keys")
    return dict(grouped)


def simulate_reduce_phase(grouped_data: dict, job_id: int) -> dict:
    """
    REDUCE PHASE: For each key, combine all values into a single result.
    In real MapReduce: result is written to HDFS (disk).
    """
    print(f"\n[Job {job_id}] --- REDUCE PHASE ---")

    result = {}
    for key, values in grouped_data.items():
        result[key] = sum(values)

    print(f"  Output: {len(result)} word counts")
    return result


def fake_disk_write(data: dict, step_name: str) -> str:
    """
    Simulates writing intermediate data to disk (like MapReduce does).
    Returns the file path written.
    """
    tmp = tempfile.NamedTemporaryFile(
        mode='w',
        suffix='.json',
        prefix=f'mapreduce_{step_name}_',
        delete=False
    )
    json.dump(data, tmp)
    tmp.close()
    print(f"  [DISK WRITE] Intermediate result written to: {tmp.name}")
    return tmp.name


def fake_disk_read(file_path: str, step_name: str) -> dict:
    """
    Simulates reading intermediate data from disk (like MapReduce does).
    """
    print(f"  [DISK READ]  Reading from disk for step: {step_name}")
    with open(file_path, 'r') as f:
        return json.load(f)


# ─────────────────────────────────────────────────────────────
# PART 2: Show the COST of chained MapReduce jobs
# ─────────────────────────────────────────────────────────────

def run_mapreduce_pipeline():
    """
    Runs a 3-step MapReduce pipeline (simulating chained jobs):
      Job 1: Word Count
      Job 2: Filter words with count > 2
      Job 3: Sort by frequency (descending)

    Each step reads from 'disk' and writes to 'disk'.
    Watch the DISK READ/WRITE messages — this is the bottleneck.
    """
    print("=" * 60)
    print("MAPREDUCE PIPELINE SIMULATION")
    print("(Simulating 3 chained MapReduce jobs)")
    print("=" * 60)

    temp_files = []

    # ── JOB 1: Word Count ──────────────────────────────────────
    print("\n>>> JOB 1: Word Count <<<")
    start = time.time()

    mapped = simulate_map_phase(SAMPLE_DATA, job_id=1)
    grouped = simulate_shuffle_sort(mapped, job_id=1)
    word_counts = simulate_reduce_phase(grouped, job_id=1)

    # DISK WRITE — mandatory in MapReduce!
    path1 = fake_disk_write(word_counts, "job1_output")
    temp_files.append(path1)
    job1_time = time.time() - start
    print(f"  Job 1 complete in {job1_time:.4f}s")

    # ── JOB 2: Filter (count > 2) ──────────────────────────────
    print("\n>>> JOB 2: Filter words with count > 2 <<<")
    start = time.time()

    # DISK READ — must re-read job 1's output from disk!
    job1_result = fake_disk_read(path1, "Filter Job")
    filtered = {w: c for w, c in job1_result.items() if c > 2}

    path2 = fake_disk_write(filtered, "job2_output")
    temp_files.append(path2)
    job2_time = time.time() - start
    print(f"  Job 2 complete in {job2_time:.4f}s")
    print(f"  Words appearing > 2 times: {len(filtered)}")

    # ── JOB 3: Sort by frequency ───────────────────────────────
    print("\n>>> JOB 3: Sort by frequency (top 10) <<<")
    start = time.time()

    # DISK READ — must re-read job 2's output from disk!
    job2_result = fake_disk_read(path2, "Sort Job")
    sorted_result = sorted(job2_result.items(), key=lambda x: x[1], reverse=True)

    path3 = fake_disk_write(dict(sorted_result[:10]), "job3_final")
    temp_files.append(path3)
    job3_time = time.time() - start
    print(f"  Job 3 complete in {job3_time:.4f}s")

    # ── Results ────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("TOP 10 WORDS (by frequency):")
    print("=" * 60)
    for word, count in sorted_result[:10]:
        bar = "█" * count
        print(f"  {word:<15} {count:3d}  {bar}")

    # ── Count the pain ────────────────────────────────────────
    print("\n" + "=" * 60)
    print("DISK I/O COST SUMMARY")
    print("=" * 60)
    print(f"  Disk WRITES: 3  (one per job)")
    print(f"  Disk READS : 2  (each job reads previous job's output)")
    print(f"  Total disk ops: 5 for a simple 3-step pipeline!")
    print(f"\n  In real MapReduce on a cluster:")
    print(f"  → These reads/writes go across slow HDDs or network storage")
    print(f"  → Latency = 10ms per seek × millions of blocks = SLOW")

    # Cleanup temp files
    for f in temp_files:
        os.unlink(f)


# ─────────────────────────────────────────────────────────────
# PART 3: Simulate Iterative ML — Show Exponential Cost
# ─────────────────────────────────────────────────────────────

def show_iterative_ml_cost():
    """
    Machine learning requires running the same computation many times (iterations).
    Shows how MapReduce disk cost compounds vs Spark's in-memory approach.
    """
    print("\n" + "=" * 60)
    print("ITERATIVE ML COST: MapReduce vs Spark")
    print("=" * 60)

    num_iterations = 20
    disk_read_time_ms = 100    # ms per disk read (simplified)
    disk_write_time_ms = 100   # ms per disk write
    memory_time_ms = 1         # ms per memory read (100x faster)

    print(f"\nAssumptions:")
    print(f"  Disk read/write: {disk_read_time_ms}ms each")
    print(f"  Memory read    : {memory_time_ms}ms")
    print(f"  ML iterations  : {num_iterations}")

    print(f"\n{'Iteration':<12} {'MapReduce (disk)':<25} {'Spark (memory)'}")
    print("-" * 55)

    mr_total = 0
    spark_total = 0

    for i in range(1, num_iterations + 1):
        # MapReduce: read + process + write per iteration
        mr_cost = disk_read_time_ms + disk_write_time_ms
        mr_total += mr_cost

        # Spark: data already in memory after first load
        if i == 1:
            spark_cost = disk_read_time_ms + memory_time_ms  # first load from disk
        else:
            spark_cost = memory_time_ms  # subsequent iterations from RAM
        spark_total += spark_cost

        print(f"  {i:<10} {mr_total:<25}ms {spark_total}ms")

    print("\n" + "=" * 60)
    print(f"  Total MapReduce time : {mr_total}ms ({mr_total/1000:.1f}s)")
    print(f"  Total Spark time     : {spark_total}ms ({spark_total/1000:.3f}s)")
    print(f"  Spark speedup        : {mr_total/spark_total:.1f}x faster")
    print("=" * 60)
    print("\nThis is why Spark dominates for ML workloads!")


# ─────────────────────────────────────────────────────────────
# PART 4: The MapReduce Word Count you'd write in Python
# (contrast with PySpark version in next topics)
# ─────────────────────────────────────────────────────────────

def pure_python_word_count():
    """
    This is roughly what you'd implement to simulate MapReduce.
    Notice how explicit and manual everything is.
    Compare this with the PySpark version you'll write in Topic 5:
        rdd.flatMap(lambda line: line.split())
           .map(lambda word: (word, 1))
           .reduceByKey(lambda a, b: a + b)
    That's it. 3 lines vs all of this.
    """
    print("\n" + "=" * 60)
    print("PURE PYTHON 'MapReduce' vs PySpark (preview)")
    print("=" * 60)

    # Step 1: Map
    pairs = []
    for line in SAMPLE_DATA:
        for word in line.lower().split():
            pairs.append((word, 1))

    # Step 2: Group (Shuffle simulation)
    grouped = defaultdict(list)
    for k, v in pairs:
        grouped[k].append(v)

    # Step 3: Reduce
    result = {k: sum(v) for k, v in grouped.items()}

    print("\nPure Python MapReduce word count: DONE")
    print("Lines of code for Map+Shuffle+Reduce: ~15 lines")
    print("\nEquivalent PySpark (you'll write this in Topic 5):")
    print("""
    spark = SparkSession.builder.getOrCreate()
    result = (
        spark.sparkContext
             .parallelize(SAMPLE_DATA)
             .flatMap(lambda line: line.split())
             .map(lambda word: (word, 1))
             .reduceByKey(lambda a, b: a + b)
             .sortBy(lambda x: x[1], ascending=False)
             .collect()
    )
    """)
    print("Lines of code: 8 lines (including session setup)")
    print("And it runs distributed across a cluster automatically.")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_mapreduce_pipeline()
    show_iterative_ml_cost()
    pure_python_word_count()

    print("\n" + "=" * 60)
    print("KEY TAKEAWAYS")
    print("=" * 60)
    print("""
  1. MapReduce = disk write after EVERY step → slow for pipelines
  2. MapReduce = disk read/write per iteration → terrible for ML
  3. Spark = keeps data in RAM → 10-100x faster
  4. Spark API = much simpler than raw MapReduce code
  5. Spark = batch + streaming + SQL + ML in ONE engine
    """)
