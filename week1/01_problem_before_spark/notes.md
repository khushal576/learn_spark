# Topic 1: The Problem Before Spark — Hadoop & MapReduce

> Phase 1 → Week 1 → Topic 1

---

## Why This Topic Matters for Interviews

Almost every Spark interview starts with: *"Why was Spark created?"*
If you can't explain what came before, you can't explain WHY Spark makes the choices it does
(in-memory processing, DAG, lazy eval, etc.). This is your foundation.

---

## 1. The Problem: Data Got Huge

Imagine you run a library with 1 million books. You need to count how many times
the word "dragon" appears across all books.

- **One person, one book at a time** → takes forever. This is a single machine.
- **Split the books across 1000 people, each counts their pile, then combine** → fast.
  This is the idea behind distributed computing.

In the real world: companies like Google, Yahoo, Facebook had **terabytes → petabytes** of
log data, click data, transaction data. A single machine couldn't store or process it.

---

## 2. The Solution (2004–2012): Hadoop & MapReduce

### 2.1 What is Hadoop?

Hadoop is a **framework** for distributed storage and processing of big data.
It has two main components:

| Component | Full Name | What it does |
|-----------|-----------|--------------|
| HDFS | Hadoop Distributed File System | Stores data split across many machines |
| MapReduce | MapReduce Engine | Processes data across those machines |

Think of **HDFS** as a giant shared hard drive spread across hundreds of servers.
Think of **MapReduce** as the factory process that runs on that data.

### 2.2 HDFS — How Data is Stored

```
Big file (10 GB)
       │
       ▼
  Split into blocks (default 128 MB each)
       │
  ┌────┴─────────────────────────────────────────┐
  │  Block 1   Block 2   Block 3   Block 4 ...   │
  └──────┬───────┬──────────┬──────────┬─────────┘
         │       │          │          │
     Node 1   Node 2    Node 3    Node 4
```

- Each block is **replicated 3x** for fault tolerance
- A **NameNode** tracks which block lives where (metadata server)
- **DataNodes** are the actual storage machines

### 2.3 MapReduce — The Processing Model

MapReduce has exactly **2 phases**:

```
INPUT DATA
    │
    ▼
┌─────────┐
│  MAP    │  ← Each worker reads its local data, produces key-value pairs
└────┬────┘
     │  (key, value) pairs
     ▼
┌─────────┐
│SHUFFLE  │  ← All same keys sent to the same reducer (NETWORK I/O!)
│ & SORT  │
└────┬────┘
     │
     ▼
┌─────────┐
│ REDUCE  │  ← Combine all values for each key → write result to HDFS
└────┬────┘
     │
     ▼
OUTPUT (written to HDFS disk)
```

#### Real Example: Word Count

Input file:
```
"hello world hello spark"
"spark is fast spark"
```

**Map Phase** — each word becomes a (word, 1) pair:
```
("hello", 1), ("world", 1), ("hello", 1), ("spark", 1)
("spark", 1), ("is", 1), ("fast", 1), ("spark", 1)
```

**Shuffle Phase** — group by key:
```
"hello" → [1, 1]
"world" → [1]
"spark" → [1, 1, 1]
"is"    → [1]
"fast"  → [1]
```

**Reduce Phase** — sum each list:
```
("hello", 2)
("world", 1)
("spark", 3)
("is", 1)
("fast", 1)
```

---

## 3. The Problems with MapReduce (Why Spark Was Needed)

### Problem 1: Disk I/O After Every Step — The #1 Killer

This is the most important problem to understand.

Every MapReduce job:
1. Reads data from HDFS (disk read)
2. Processes it
3. **Writes intermediate results back to HDFS** (disk write)
4. Next job reads from HDFS again (disk read)
5. Processes it
6. Writes to HDFS again (disk write)

```
Job 1:  HDFS → RAM → process → HDFS (write)
Job 2:  HDFS → RAM → process → HDFS (write)
Job 3:  HDFS → RAM → process → HDFS (write)
...
```

Disk is **100x slower** than RAM. If you have 10 chained MapReduce jobs,
you're paying that penalty 10 times.

**Analogy**: Imagine you're solving a math problem. After every step, you
erase your paper, take a photo, print the photo, then re-read the printed
photo for the next step. Insane, right? That's MapReduce.

### Problem 2: No In-Memory Computation

MapReduce has no concept of keeping data in memory between jobs.
Each job starts fresh from disk.

This makes **iterative algorithms** (like machine learning) extremely slow:

```
ML Training = run the same computation 100+ times (iterations)

With MapReduce:
  Iteration 1: read disk → process → write disk
  Iteration 2: read disk → process → write disk
  ... × 100
  = 200 disk reads/writes per model training!

With Spark:
  Load data into memory once → iterate in memory × 100
  = 2 disk reads/writes total
```

### Problem 3: Complex Multi-Step Pipelines Are Painful

A real ETL pipeline might look like:
1. Parse raw logs
2. Filter out bot traffic
3. Join with user table
4. Aggregate by region
5. Calculate percentiles
6. Write output

In MapReduce: each step = a separate MapReduce job = separate disk write/read.
You need to orchestrate all of these yourself. Error handling, re-running failed steps = your problem.

In Spark: all steps are a **DAG** executed as one logical job, in memory.

### Problem 4: Only Batch Processing

MapReduce is **batch-only**. You cannot process streaming data (events as they arrive).
For real-time analytics, you needed completely separate systems (Storm, etc.).

Spark unified batch + streaming under one API.

### Problem 5: Verbose API

MapReduce code in Java is extremely verbose. A simple word count = ~100 lines.
PySpark word count = 5 lines.

---

## 4. The Hadoop Ecosystem That Grew Around It

Because MapReduce was so low-level, many tools were built on top of it:

| Tool | Purpose | Spark Equivalent |
|------|---------|-----------------|
| Hive | SQL on Hadoop | Spark SQL |
| Pig | Scripting language | DataFrame API |
| HBase | NoSQL on HDFS | - |
| Mahout | ML on MapReduce | MLlib |
| Storm | Real-time streaming | Spark Streaming |
| Oozie | Workflow scheduler | Airflow |

Spark replaced most of these with **one unified engine**.

---

## 5. The Birth of Spark (2009)

Spark was created at **UC Berkeley's AMPLab** in 2009 by **Matei Zaharia**.
He was frustrated by how slow iterative algorithms were on Hadoop.

Key insight: **Keep data in RAM between computations.**

Spark was open-sourced in 2010, donated to Apache in 2013, and became
Apache Spark in 2014. It's now the de-facto standard for big data processing.

### Spark vs Hadoop MapReduce — Speed

Spark is:
- **10-100x faster** for batch workloads (in-memory vs disk)
- **1000x faster** for iterative ML workloads

The Berkeley benchmarks (2014) showed Spark sorting 100TB of data
**3x faster than Hadoop** using **10x fewer machines**.

---

## 6. What Spark Kept from Hadoop

Spark didn't throw everything away:

- Still runs on **HDFS** for storage (or S3, GCS, Azure Blob)
- Still runs on **YARN** (Hadoop's resource manager) — or Kubernetes
- Still uses the concept of **distributed workers**
- Still fault-tolerant (but via lineage, not replication of intermediate data)

---

## 7. Interview Cheat Sheet for This Topic

**Q: What was wrong with MapReduce?**
> MapReduce writes intermediate data to disk after every step, making multi-step
> pipelines extremely slow. It has no concept of in-memory processing between jobs,
> making iterative algorithms (like ML training) impractical. It only supports batch
> processing, has a rigid two-phase model (Map → Reduce), and requires complex
> orchestration for multi-step pipelines.

**Q: Why is disk I/O slow?**
> Disk read/write speed is ~100-150 MB/s for HDDs, ~500 MB/s for SSDs.
> RAM bandwidth is ~50 GB/s — roughly 100-300x faster. MapReduce's model of
> writing intermediate results to disk made it pay this penalty repeatedly.

**Q: What problem did Spark solve?**
> Spark keeps intermediate data in memory (RAM) across computation steps using
> its RDD abstraction, eliminating the disk I/O penalty. It also provides a
> richer API, unified batch + streaming, and supports iterative algorithms efficiently.

**Q: Is Hadoop dead?**
> Hadoop as a storage system (HDFS) is still used, but being replaced by cloud
> object stores (S3, GCS). Hadoop MapReduce as a processing engine is largely
> replaced by Spark. YARN as a resource manager is still used but Kubernetes is
> growing. The Hadoop ecosystem gave birth to the modern data lakehouse.

---

## 8. Summary

```
Problem                          → Solution
─────────────────────────────────────────────────────────
Data too big for one machine     → Distributed storage (HDFS)
Single machine too slow          → Distributed processing (MapReduce)
MapReduce disk I/O too slow      → In-memory processing (Spark RDDs)
MapReduce batch only             → Unified batch + streaming (Spark)
Complex ecosystem of tools       → One unified engine (Spark)
```

---

## Next Topic
**Topic 2: Spark Architecture — Driver, Cluster Manager, Workers, Executors**
