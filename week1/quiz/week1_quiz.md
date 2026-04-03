# Week 1 Quiz — Phase 1: Foundation

> Complete all 5 topics before attempting this quiz.
> Answers at the bottom — no peeking!

---

## Section 1: The Problem Before Spark (Topic 1)

**Q1.** In MapReduce, what happens between each MapReduce job in a multi-step pipeline?
- a) Data is kept in memory for the next job
- b) Data is written to HDFS (disk), then read back for the next job
- c) Data is sent over the network to the Driver
- d) Data is replicated 3× to prevent loss

**Q2.** Why is Spark especially faster than MapReduce for machine learning tasks?
- a) Spark uses Python instead of Java
- b) Spark uses better sorting algorithms
- c) Spark keeps data in memory across iterations, avoiding repeated disk I/O
- d) Spark uses more CPU cores by default

**Q3.** Which of the following is NOT a problem with Hadoop MapReduce?
- a) Disk I/O between every step
- b) No in-memory processing
- c) Cannot run on distributed systems
- d) Batch-only (no streaming support)

**Q4 (Short Answer).** A company runs a 10-step ETL pipeline in MapReduce. How many disk reads and writes happen during the pipeline? (Assume each step is a separate MapReduce job.)

**Q5 (Short Answer).** What is the main storage component of Hadoop and how does it store large files?

---

## Section 2: Spark Architecture (Topic 2)

**Q6.** Which component of Spark decides how to split a Job into Stages and Tasks?
- a) Cluster Manager
- b) Executor
- c) Worker Node
- d) Driver (DAGScheduler + TaskScheduler)

**Q7.** You have a Spark cluster with 4 Worker Nodes, each with 1 Executor and 4 cores.
How many Tasks can run in parallel at maximum?
- a) 4
- b) 8
- c) 16
- d) 1

**Q8.** What happens when an Executor dies mid-job?
- a) The entire application crashes and must be restarted
- b) The Driver crashes and all work is lost
- c) The Driver reschedules the failed tasks on other executors, recomputing lost partitions from lineage
- d) The Cluster Manager automatically saves the data to HDFS

**Q9.** A colleague says "I'll run my Spark job in Client mode for production." What is the risk?
- a) Client mode is slower because the Driver is remote
- b) Client mode does not support YARN
- c) If the client machine disconnects, the job fails because the Driver is on that machine
- d) Client mode doesn't support DataFrames

**Q10 (Short Answer).** What is the difference between a Worker Node and an Executor? Use an analogy.

---

## Section 3: Jobs, Stages, Tasks + SparkSession (Topic 3)

**Q11.** How many Spark Jobs does the following code trigger?
```python
df = spark.read.csv("data.csv")
filtered = df.filter(df.age > 25)
count = filtered.count()
filtered.show()
filtered.write.parquet("output/")
```
- a) 1
- b) 2
- c) 3
- d) 5

**Q12.** How many Stages will this query create?
```python
df.filter(df.salary > 50000) \
  .groupBy("dept") \
  .count() \
  .orderBy("count") \
  .show()
```
- a) 1
- b) 2
- c) 3
- d) 4

**Q13.** What does `SparkSession.builder.getOrCreate()` do if a SparkSession already exists?
- a) Creates a second, separate SparkSession
- b) Throws an error
- c) Returns the existing SparkSession without creating a new one
- d) Restarts the existing SparkSession

**Q14.** You have 200 partitions and 20 executor cores. How many "waves" of tasks will Spark need?
- a) 10
- b) 20
- c) 200
- d) 4000

**Q15 (Short Answer).** What is the relationship between number of partitions and number of tasks?

---

## Section 4: DAG (Topic 4)

**Q16.** When does Spark actually build and optimize the DAG?
- a) When each transformation is called
- b) When the SparkSession is created
- c) When an Action is called — Spark then optimizes the full plan before executing
- d) When `spark.sql.optimize = True` is set

**Q17.** What is "Predicate Pushdown" in the Catalyst Optimizer?
- a) Pushing sort operations to the end of the plan
- b) Moving filter conditions closer to the data source so fewer rows are processed upstream
- c) Pre-computing joins before reading data
- d) Splitting the DAG at shuffle boundaries

**Q18.** In the Spark UI DAG visualization, what does an "Exchange" node represent?
- a) Reading data from S3
- b) A cache hit
- c) A shuffle operation (stage boundary)
- d) A broadcast join

**Q19.** You run `df.explain()` and see this (simplified):
```
== Physical Plan ==
*(2) Sort [count ASC]
+- Exchange rangepartitioning(count ASC, 4)
   +- *(1) HashAggregate
      +- Exchange hashpartitioning(dept, 4)
         +- *(1) Filter (salary > 50000)
            +- Scan
```
How many stages are there and where are the boundaries?
- a) 1 stage (Scan → Filter → HashAggregate → Sort)
- b) 2 stages (boundary after first Exchange)
- c) 3 stages (boundary after each Exchange)
- d) 4 stages (one per operator)

**Q20 (Short Answer).** How does the DAG provide fault tolerance without requiring intermediate data to be written to disk?

---

## Section 5: Transformations vs Actions (Topic 5)

**Q21.** Which of the following is a WIDE transformation?
- a) `df.filter(df.age > 25)`
- b) `df.withColumn("tax", df.salary * 0.3)`
- c) `df.select("name", "salary")`
- d) `df.groupBy("dept").count()`

**Q22.** Why is `reduceByKey` preferred over `groupByKey` for aggregations?
- a) reduceByKey uses less code
- b) reduceByKey reduces data locally within each partition before shuffling, sending less data across the network
- c) groupByKey doesn't support Python lambdas
- d) reduceByKey automatically caches the result

**Q23.** A data engineer writes this code:
```python
df = spark.read.csv("huge_file.csv")
df2 = df.filter(df.country == "India")
df3 = df2.select("name", "salary")
df4 = df3.withColumn("tax", df3.salary * 0.3)
result = df4.collect()
```
What are the two problems with this code?

**Q24.** What does `rdd.toDebugString()` show you?
- a) The Java bytecode for the RDD operations
- b) The current memory usage of each partition
- c) The lineage chain — how the RDD was derived from its parent RDDs
- d) The physical execution plan after Catalyst optimization

**Q25 (Short Answer).** Explain lazy evaluation in one sentence and give one benefit it provides.

---

## BONUS: Scenario Questions (Interview Style)

**B1.** You work at a company where a nightly Spark job reads 500GB of data, joins with a
lookup table, aggregates, and writes results. The job takes 3 hours. Your manager asks you
to make it faster. What are THREE things you would investigate first?
(Think about what you've learned in Week 1.)

**B2.** A junior engineer on your team says: "I'll just write the whole pipeline as a single
Python script with a for loop that calls `df.filter(condition).count()` 100 times for
different conditions." What is wrong with this approach and how would you fix it?

**B3.** Design question: You need to process 10 years of weblogs (100TB) to find the top
1000 most-visited URLs per month. At a high level, describe how this would work in Spark —
what stages would you expect to see?

---

## ANSWERS

<details>
<summary>Click to reveal answers</summary>

**Q1: b** — Data is written to HDFS between every job.

**Q2: c** — In-memory processing between iterations. ML trains over the same data 100+ times.

**Q3: c** — MapReduce absolutely runs on distributed systems (that's its whole point).

**Q4:** 10 jobs × 1 write + 9 reads (first job reads from input, not from a previous job's output) = 10 writes + 9 reads = 19 disk operations minimum. (In practice each job does 1 read + 1 write = 20 total, minus the first job reading input.)

**Q5:** HDFS (Hadoop Distributed File System). It splits large files into blocks (default 128MB), distributes blocks across DataNodes, and replicates each block 3× for fault tolerance. A NameNode tracks metadata (which block is where).

**Q6: d** — The Driver's DAGScheduler splits the job into stages and the TaskScheduler assigns tasks to executors.

**Q7: c** — 4 Workers × 1 Executor × 4 cores = 16 parallel tasks.

**Q8: c** — Driver reschedules failed tasks on other executors. The failed tasks' partitions are recomputed from the lineage.

**Q9: c** — In Client mode the Driver is on the submitting machine. If that machine disconnects or crashes, the job fails.

**Q10:** Worker Node = the physical/virtual machine (the building). Executor = the JVM process running on that machine (an employee in the building). One Worker Node can host one or more Executors. The Worker Node manages the hardware; the Executor actually runs tasks.

**Q11: c** — 3 actions: `.count()`, `.show()`, `.write.parquet()` = 3 Jobs. The read and filter are lazy.

**Q12: c** — 3 stages. `filter` is narrow (Stage 0 continues). `groupBy` creates a shuffle (Stage 1). `orderBy` creates another shuffle (Stage 2).

**Q13: c** — Returns the existing session. This is the "safe singleton" pattern.

**Q14: a** — 200 partitions ÷ 20 cores = 10 waves.

**Q15:** One task is created per partition, per stage. If a stage has 100 partitions → 100 tasks. They run in parallel waves limited by the total number of executor cores.

**Q16: c** — DAG is optimized when an Action is called. Transformations just build the logical plan.

**Q17: b** — Predicate Pushdown moves filter conditions as close to the data source as possible, reducing rows that flow through the rest of the pipeline.

**Q18: c** — Exchange = shuffle. Stage boundaries appear at Exchange nodes.

**Q19: c** — 3 stages. Each Exchange (hashpartitioning, rangepartitioning) is a shuffle = stage boundary. Scan+Filter = Stage 1, HashAggregate = Stage 2, Sort = Stage 3.

**Q20:** The DAG (lineage) records HOW every partition was computed — the full chain of transformations. If a partition is lost, Spark re-runs only the transformations in the lineage for that specific partition, starting from the original data source. No intermediate disk storage needed.

**Q21: d** — `groupBy()` is wide (requires shuffle to group all rows by the same key). The others are narrow.

**Q22: b** — reduceByKey performs a local reduction within each partition before shuffling, so much less data crosses the network.

**Q23:** Problem 1: `collect()` on a huge CSV brings ALL data to Driver RAM → OOM. Problem 2: No caching — if `df4` is used multiple times, Spark re-reads and reprocesses the entire CSV each time.

**Q24: c** — `toDebugString()` shows the lineage chain of RDD dependencies.

**Q25:** Lazy evaluation means transformations are not executed when called — they build a plan (DAG) that only executes when an Action is called. Benefit: Spark can see the complete plan and apply optimizations (predicate pushdown, column pruning) before any data is moved.

**B1 — Three things to investigate:**
1. Are there unnecessary shuffles? (Look at the explain() plan — how many Exchange nodes?)
2. Is the data being re-read multiple times? (Cache intermediate results if used more than once)
3. What is `spark.sql.shuffle.partitions` set to? (Default 200 may be wrong for 500GB)

**B2 — Problems and fix:**
Problem: Each `.filter().count()` triggers a separate Job, re-reading the input 100 times.
Fix: Read once, cache the DataFrame, then run all counts. Or restructure using a single groupBy/pivot to answer all 100 conditions in one pass.

**B3 — Expected stages:**
- Stage 0: Read raw logs, extract URL + timestamp → parse month
- Shuffle 1: groupBy(month, URL) → Stage 1: count visits per URL per month
- Shuffle 2: repartition by month + rank → Stage 2: window function rank() or orderBy within each month
- Filter: keep rank <= 1000
- Write: partitionBy("month") to output

</details>
