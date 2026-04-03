# Apache Spark & PySpark — 7-Phase Mastery Course

> From zero to pipeline architect.
> Every concept. Every interview question. Every production pattern.

---

## Quick Start

```bash
# 1. Clone and enter the repo
cd learn_spark

# 2. Start the full Spark cluster
docker-compose up -d

# 3. Open your learning environment
#    Jupyter Notebook  → http://localhost:8888
#    Spark Master UI   → http://localhost:8080
#    Worker 1 UI       → http://localhost:8081
#    Worker 2 UI       → http://localhost:8082

# 4. Run your first script
docker-compose exec jupyter python /workspace/week1/01_problem_before_spark/mapreduce_simulation.py

# 5. Open Spark App UI (while a script is running)
#    http://localhost:4040
```

Or run locally without Docker:
```bash
pip install -r requirements.txt
python week1/01_problem_before_spark/mapreduce_simulation.py
```

---

## Repository Structure

```
learn_spark/
├── README.md                          ← This file
├── Dockerfile                         ← PySpark + Jupyter environment
├── docker-compose.yml                 ← Spark cluster (master + 2 workers + jupyter)
├── requirements.txt                   ← Python dependencies
├── .gitignore
│
├── week1/                             ← Phase 1: Foundation  ✅ DONE
│   ├── README.md
│   ├── 01_problem_before_spark/
│   │   ├── notes.md                   ← Concept + analogy + interview Qs
│   │   └── mapreduce_simulation.py    ← Runnable demo
│   ├── 02_spark_architecture/
│   │   ├── notes.md
│   │   └── architecture_demo.py
│   ├── 03_sparksession_and_sparkcontext/
│   │   ├── notes.md
│   │   └── session_demo.py
│   ├── 04_dag_lazy_evaluation/
│   │   ├── notes.md
│   │   └── dag_demo.py
│   ├── 05_transformations_vs_actions/
│   │   ├── notes.md
│   │   └── transforms_demo.py
│   └── quiz/
│       └── week1_quiz.md
│
├── week2/                             ← Phase 2: RDDs  ✅ DONE
│   ├── README.md
│   ├── 01_rdd_fundamentals.ipynb
│   ├── 02_rdd_transformations.ipynb
│   ├── 03_rdd_actions.ipynb
│   ├── 04_pair_rdds.ipynb
│   ├── 05_wide_vs_narrow_shuffle.ipynb
│   └── quiz/
│       └── week2_quiz.md
│
├── week3/                             ← Phase 3: DataFrames & Spark SQL (Part 1)
├── week4/                             ← Phase 3: DataFrames & Spark SQL (Part 2)
├── week5/                             ← Phase 3: DataFrames & Spark SQL (Part 3)
├── week6/                             ← Phase 4: Performance & Optimization (Part 1)
├── week7/                             ← Phase 4: Performance & Optimization (Part 2)
├── week8/                             ← Phase 5: PySpark ETL Engineering (Part 1)
├── week9/                             ← Phase 5: PySpark ETL Engineering (Part 2)
├── week10/                            ← Phase 6: Spark Streaming (Part 1)
├── week11/                            ← Phase 6: Spark Streaming (Part 2)
└── week12+/                           ← Phase 7: Advanced & Master Level
```

---

## 7-Phase Course Roadmap

### Phase 1 — Foundation ✅ DONE → Week 1

| Week | Status | Topics |
|------|--------|--------|
| **Week 1** | ✅ Done | The problem before Spark (Hadoop, MapReduce, disk I/O), Spark Architecture (Driver/Cluster Manager/Workers/Executors), Tasks/Stages/Jobs, SparkContext vs SparkSession, DAG + Catalyst Optimizer, Lazy Evaluation, Transformations vs Actions, Lineage |

### Phase 2 — RDDs ✅ DONE → Week 2

| Week | Status | Topics |
|------|--------|--------|
| **Week 2** | ✅ Done | RDD fundamentals + 5 properties + partitions + immutability, map/flatMap/filter/distinct/union/mapPartitions, collect/count/reduce/fold/aggregate/foreach, Pair RDDs: reduceByKey/groupByKey/sortByKey/join/aggregateByKey, Wide vs Narrow + Shuffle internals + data skew + salting |

### Phase 3 — DataFrames & Spark SQL → Weeks 3-5 ← YOU ARE HERE NEXT

| Week | Status | Topics |
|------|--------|--------|
| **Week 3** | ⬜ Next | DataFrame vs RDD, Schema (StructType, StructField), Reading CSV / JSON / Parquet / ORC, select / filter / withColumn / groupBy / agg |
| **Week 4** | ⬜ | All join types (inner / left / right / full / semi / anti), Window Functions (rank, dense_rank, lead, lag, row_number) |
| **Week 5** | ⬜ | UDFs, built-in functions, Spark SQL (createOrReplaceTempView, CTEs, subqueries) |

### Phase 4 — Performance & Optimization → Weeks 6-7

| Week | Status | Topics |
|------|--------|--------|
| **Week 6** | ⬜ | Partitioning (repartition vs coalesce, skew, salting), Caching & Persistence (cache vs persist, all storage levels, unpersist), Shuffle optimization, spark.sql.shuffle.partitions, bucket joins |
| **Week 7** | ⬜ | AQE (Adaptive Query Execution), Spark UI deep dive (spill/skew detection), explain() (physical vs logical plan), Catalyst Optimizer, Tungsten Engine, Memory management + OOM debugging |

### Phase 5 — PySpark ETL Engineering → Weeks 8-9

| Week | Status | Topics |
|------|--------|--------|
| **Week 8** | ⬜ | SparkSession config, reading from S3/HDFS, writing with partitionBy(), schema enforcement, bad record handling, Medallion Architecture (Bronze → Silver → Gold) |
| **Week 9** | ⬜ | Idempotent pipelines, incremental vs full loads, watermarking, pipeline architecture (error handling, dead letter queues, logging, checkpointing), Delta Lake (ACID, time travel, MERGE INTO, schema evolution, OPTIMIZE, ZORDER, Vacuum) |

### Phase 6 — Spark Streaming → Weeks 10-11

| Week | Status | Topics |
|------|--------|--------|
| **Week 10** | ⬜ | Structured Streaming (micro-batch vs continuous, trigger options), input sources & output sinks, stateless vs stateful, watermarking for late data |
| **Week 11** | ⬜ | Window operations (tumbling, sliding, session), Kafka + Spark (reading/writing, exactly-once semantics) |

### Phase 7 — Advanced & Master Level → Weeks 12+

| Week | Status | Topics |
|------|--------|--------|
| **Week 12** | ⬜ | Delta Lake deep dive (MERGE INTO, ZORDER, Vacuum, change data feed), cluster config (executor count, cores, memory, dynamic allocation) |
| **Week 13** | ⬜ | Databricks vs EMR vs local, advanced joins (bloom filters, broadcast hash join vs sort-merge join), vectorized UDFs (pandas UDFs), Arrow optimization |
| **Week 14** | ⬜ | AWS: EMR + S3 + Glue Catalog integration, orchestration with Apache Airflow |
| **Week 15** | ⬜ | CI/CD for Spark, unit testing with pytest, data quality checks in pipelines |
| **Week 16** | ⬜ | Capstone: design and build a full production pipeline end-to-end |

---

## How to Use This Repo

### Daily Study Pattern (1-2 hours/day)

```
Week 1 (Phase 1 — .py + notes.md format):
  Day 1:  Read notes.md for the topic
  Day 2:  Run the .py demo script, explore Spark UI at localhost:4040
  Day 3:  Do the exercises at the bottom of notes.md

Week 2+ (Phase 2 onwards — .ipynb format):
  Day 1:  Open the notebook, read all Markdown cells (notes + analogy)
  Day 2:  Run all Code cells top-to-bottom, observe outputs
  Day 3:  Redo the exercises at the bottom of the notebook from scratch
  After all notebooks in a week: Take the quiz in weekN/quiz/
```

### For Interview Prep

Every notebook (Week 2+) and every `notes.md` (Week 1) has an **Interview Cheat Sheet** section.
Read these the night before an interview. They contain:
- The exact question (as interviewers phrase it)
- The ideal answer (complete but concise)

### For Work Reference

When stuck on a problem at work:
1. Identify the Phase (Performance? Streaming? ETL?)
2. Find the relevant week's folder
3. Check notes.md for the concept
4. Check the demo script for a working code example

---

## Spark Version

This course uses **Apache Spark 3.5.0** with:
- Python 3.11
- Delta Lake 3.0.0
- Java 17

---

## Spark UI Ports

| Service | URL | Purpose |
|---------|-----|---------|
| Jupyter | http://localhost:8888 | Write and run code |
| Spark Master | http://localhost:8080 | Cluster overview |
| Worker 1 | http://localhost:8081 | Worker stats |
| Worker 2 | http://localhost:8082 | Worker stats |
| App UI | http://localhost:4040 | Jobs, Stages, Tasks (while app runs) |

---

## Key Commands

```bash
# Start cluster
docker-compose up -d

# Stop cluster
docker-compose down

# Run a script
docker-compose exec jupyter python /workspace/week1/02_spark_architecture/architecture_demo.py

# Open bash in the Jupyter container
docker-compose exec jupyter bash

# See logs
docker-compose logs -f spark-master
docker-compose logs -f jupyter

# Rebuild if Dockerfile changes
docker-compose build
docker-compose up -d
```

---

## Spark Quick Reference

### Create SparkSession
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

sc = spark.sparkContext   # access SparkContext
spark.sparkContext.setLogLevel("WARN")
```

### Common Transformations (Lazy)
```python
df.filter(df.col > 100)           # narrow
df.select("col1", "col2")         # narrow
df.withColumn("new", df.old * 2)  # narrow
df.drop("col")                    # narrow
df.groupBy("col").count()         # wide (shuffle)
df.join(other, "key")             # wide (shuffle)
df.orderBy("col")                 # wide (shuffle)
df.distinct()                     # wide (shuffle)
df.repartition(8)                 # wide (shuffle)
df.coalesce(2)                    # narrow (no shuffle)
```

### Common Actions (Trigger Execution)
```python
df.count()                        # returns Long
df.show(20)                       # prints to console
df.collect()                      # returns list of Rows (⚠️ Driver OOM risk)
df.take(n)                        # returns list of n Rows
df.first()                        # returns first Row
df.write.parquet("path/")         # writes to disk
df.write.mode("overwrite").csv()  # overwrite output
```

### Inspect the Plan
```python
df.explain()                      # physical plan
df.explain(True)                  # all plans
df.explain(mode="formatted")      # formatted (Spark 3.x)
rdd.toDebugString()               # RDD lineage
```
