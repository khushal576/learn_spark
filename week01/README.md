# Week 1 — Phase 1: Foundation

> **Course**: Apache Spark & PySpark — 7-Phase Mastery
> **Phase**: 1 of 7
> **Week**: 1 of 2 (Foundation)

---

## Topics Covered This Week

| # | Topic | Notes | Code | Status |
|---|-------|-------|------|--------|
| 1 | The Problem Before Spark (Hadoop, MapReduce, Disk I/O) | [notes.md](01_problem_before_spark/notes.md) | [mapreduce_simulation.py](01_problem_before_spark/mapreduce_simulation.py) | |
| 2 | Spark Architecture: Driver, Cluster Manager, Workers, Executors | [notes.md](02_spark_architecture/notes.md) | [architecture_demo.py](02_spark_architecture/architecture_demo.py) | |
| 3 | Tasks, Stages, Jobs + SparkContext vs SparkSession | [notes.md](03_sparksession_and_sparkcontext/notes.md) | [session_demo.py](03_sparksession_and_sparkcontext/session_demo.py) | |
| 4 | DAG (Directed Acyclic Graph) + Catalyst Optimizer | [notes.md](04_dag_lazy_evaluation/notes.md) | [dag_demo.py](04_dag_lazy_evaluation/dag_demo.py) | |
| 5 | Lazy Evaluation, Transformations vs Actions, Lineage | [notes.md](05_transformations_vs_actions/notes.md) | [transforms_demo.py](05_transformations_vs_actions/transforms_demo.py) | |

---

## How to Study Each Topic

For each topic, follow this order:

1. **Read the notes** — understand the concept and analogy
2. **Run the demo script** — watch it execute and open Spark UI (http://localhost:4040)
3. **Modify the code** — change parameters, observe what changes
4. **Answer the check question** mentally before moving on
5. **Take the quiz** after all 5 topics

---

## Running the Code

### Option A: Docker (Recommended — full cluster)

```bash
# From the repo root
docker-compose up -d

# Wait ~30 seconds for all services to start

# Open Jupyter: http://localhost:8888
# Open Spark Master UI: http://localhost:8080

# Run a script inside the container
docker-compose exec jupyter python /workspace/week1/01_problem_before_spark/mapreduce_simulation.py
```

### Option B: Local PySpark

```bash
# Install dependencies
pip install -r requirements.txt

# Run any demo script
python week1/01_problem_before_spark/mapreduce_simulation.py
python week1/02_spark_architecture/architecture_demo.py
python week1/03_sparksession_and_sparkcontext/session_demo.py
python week1/04_dag_lazy_evaluation/dag_demo.py
python week1/05_transformations_vs_actions/transforms_demo.py
```

---

## Spark UI — Your Most Important Learning Tool

Every demo script opens the Spark UI at **http://localhost:4040**.

| Tab | What to Look For |
|-----|-----------------|
| **Jobs** | One row per Action you call |
| **Stages** | Stages within each job, shuffle read/write bytes |
| **Tasks** | Individual task details, timing, failures |
| **Storage** | Cached RDDs/DataFrames and memory usage |
| **Environment** | All Spark configuration settings |
| **Executors** | Executor memory, task counts, GC time |
| **SQL** | DataFrame/SQL query plans (visual DAG) |

**Pro tip**: When debugging a slow job, start at the Stages tab. Find the stage with the most "Shuffle Write" bytes — that's your bottleneck.

---

## Key Concepts Cheat Sheet

### Architecture

```
Your Code (Driver)
    │
    ├── DAGScheduler → splits DAG into Stages
    ├── TaskScheduler → sends Tasks to Executors
    └── SparkContext → cluster connection

Cluster Manager (YARN / K8s / Standalone)
    └── Allocates resources, launches Executors

Worker Nodes (machines)
    └── Executors (JVM processes, run Tasks)
            └── Tasks (one per partition, one per core)
```

### Job → Stage → Task

```
ACTION called
    └── JOB created
            ├── STAGE 0 (narrow transforms until first shuffle)
            │       └── N Tasks (N = partitions in this stage)
            └── STAGE 1 (after shuffle)
                    └── M Tasks (M = spark.sql.shuffle.partitions)
```

### Lazy Evaluation

```
Transformation (filter, select, groupBy...) → LAZY → just adds to DAG
Action (count, show, collect, write...)     → EAGER → triggers execution
```

### Narrow vs Wide

```
Narrow: filter, map, select, withColumn, union, coalesce
  → same stage, no shuffle, fast

Wide:   groupBy, join, orderBy, distinct, repartition, reduceByKey
  → new stage, shuffle, expensive
```

---

## Interview Questions — Week 1 Summary

These are the exact questions asked at Data Engineering interviews:

1. What was wrong with Hadoop MapReduce? Why was Spark created?
2. Explain Spark architecture: what does the Driver do? What does an Executor do?
3. What is the difference between a Worker Node and an Executor?
4. What is a DAG in Spark and why is it important?
5. What does the Catalyst Optimizer do?
6. What is lazy evaluation and why does Spark use it?
7. What is the difference between a transformation and an action?
8. What is the difference between narrow and wide transformations?
9. What is a shuffle and why is it expensive?
10. How does Spark handle fault tolerance without writing intermediate data to disk?
11. What is the difference between SparkContext and SparkSession?
12. What does `getOrCreate()` do?
13. How many tasks are created for a stage with 100 partitions?
14. What happens if you call `.collect()` on a 100GB DataFrame?
15. What is lineage in Spark?

---

## Exercises (Do These Yourself)

### Exercise 1: MapReduce Simulation
Modify `mapreduce_simulation.py` to count the top 5 words (not just all words).
What happens to the disk I/O count?

### Exercise 2: Partition Experiment
In `architecture_demo.py`, change the number of partitions and observe:
- With `local[4]`, does `repartition(4)` vs `repartition(100)` affect runtime?
- What about `repartition(1)`?

### Exercise 3: Find the Shuffles
Write a query that:
1. Reads a list of transactions
2. Filters for amounts > 100
3. Joins with a customer table
4. Groups by customer country
5. Sorts by total amount

Then call `.explain()` and count the number of Exchange nodes. How many stages are there?

### Exercise 4: Prove Lazy Evaluation
Write 5 transformations and time each one. Then call an Action and time that.
Which ones are near-zero and which one actually takes time?

### Exercise 5: reduceByKey vs groupByKey
Create an RDD with 10 million (key, 1) pairs with 3 unique keys.
Time `reduceByKey` vs `groupByKey`. How much faster is reduceByKey?

---

## Quiz

Once you finish all 5 topics, take the [Week 1 Quiz](quiz/week1_quiz.md).
Score yourself:
- 20+/25 → Ready for Week 2 (Phase 2: RDDs)
- 15-19 → Review the topics you got wrong, re-read the notes
- <15 → Re-read all notes and re-run all demos

---

## Next Week

**[Week 2](../week2/README.md): Phase 2 — RDDs (Resilient Distributed Datasets)**

Format changes from Week 2: topics are in **Jupyter Notebooks** (`.ipynb`) — notes and code in one file.

Topics:
- RDD fundamentals, 5 properties, partitions, immutability, lineage
- Transformations: map, flatMap, filter, distinct, union, mapPartitions, coalesce
- Actions: collect, count, reduce, fold, aggregate, foreach, foreachPartition
- Pair RDDs: reduceByKey, groupByKey, sortByKey, join, aggregateByKey
- Wide vs Narrow deep dive, shuffle internals, data skew, salting
