# Topic 3: Tasks, Stages, Jobs + SparkContext vs SparkSession

> Phase 1 → Week 1 → Topic 3

---

## The Movie Production Analogy

| Movie Term | Spark Term | Meaning |
|-----------|-----------|---------|
| Whole movie | **Job** | Everything triggered by one Action |
| A scene | **Stage** | A group of transformations without a shuffle |
| A single shot/take | **Task** | Work on one partition in one stage |
| Director's cut (planning) | **DAG** | The plan before any camera rolls |

---

## Part A: Jobs, Stages, and Tasks

### Job

A **Job** is created every time you call an **Action** (like `.collect()`, `.count()`, `.show()`).

- One Action = One Job
- A Spark application can have many jobs

```python
rdd.count()   # ← Action → triggers Job 1
rdd.show()    # ← Action → triggers Job 2
df.write.parquet("output")  # ← Action → triggers Job 3
```

Each job corresponds to one entry in the Spark UI → Jobs tab.

---

### Stage

A **Stage** is a group of tasks that can run without shuffling data across the network.
When Spark needs to shuffle (redistribute data across executors), it creates a **stage boundary**.

**Stages are split at shuffle boundaries.**

Narrow transformations (filter, map, select) → no shuffle → same stage
Wide transformations (groupBy, join, orderBy) → shuffle → new stage

```
Job (triggered by .count())
│
├── Stage 0: read data → filter → map    ← all narrow, no shuffle
│   ├── Task 0 (Partition 0)
│   ├── Task 1 (Partition 1)
│   ├── Task 2 (Partition 2)
│   └── Task 3 (Partition 3)
│
│   [SHUFFLE: groupBy redistributes data across executors]
│
└── Stage 1: aggregate                   ← after the shuffle
    ├── Task 0 (Shuffle Partition 0)
    ├── Task 1 (Shuffle Partition 1)
    └── Task 2 (Shuffle Partition 2)
```

**Key Rule**: The number of tasks in a stage = number of partitions in that stage.

---

### Task

A **Task** is the smallest unit of work. Each task:
- Runs on **one partition** of data
- Runs on **one executor core**
- Executes all transformations in its stage for its partition

```
Stage 0 has 4 partitions → 4 Tasks created → 4 Tasks sent to executors
```

If you have 4 executor cores and 8 partitions:
```
Wave 1: Tasks 0,1,2,3 run in parallel (using all 4 cores)
Wave 2: Tasks 4,5,6,7 run in parallel (reusing the 4 cores)
```

---

### Job → Stage → Task Hierarchy

```
Application
└── Job 1 (triggered by action 1)
│   ├── Stage 0
│   │   ├── Task 0
│   │   ├── Task 1
│   │   └── Task 2
│   └── Stage 1
│       ├── Task 0
│       └── Task 1
└── Job 2 (triggered by action 2)
    └── Stage 2
        ├── Task 0
        ├── Task 1
        ├── Task 2
        └── Task 3
```

### Real Example

```python
df = spark.read.csv("employees.csv")           # not an action — lazy
filtered = df.filter(df.salary > 50000)         # narrow transform
dept_count = filtered.groupBy("dept").count()   # wide transform (shuffle!)
dept_count.show()                               # ACTION → triggers 1 Job
```

Spark builds:
- **Stage 0**: read CSV → filter  (no shuffle needed)
- **Shuffle**: groupBy redistributes all rows by "dept" across executors
- **Stage 1**: aggregate counts per dept

One Job, Two Stages, N Tasks (N = partition count).

---

## Part B: SparkContext vs SparkSession

### The History

Before Spark 2.0 (2016), there were multiple entry points:

| Entry Point | Purpose |
|-------------|---------|
| `SparkContext (sc)` | Core Spark, RDD operations |
| `SQLContext` | Spark SQL (DataFrames) |
| `HiveContext` | Hive integration |
| `StreamingContext` | Streaming |

This was confusing. Spark 2.0 unified everything into **SparkSession**.

---

### SparkContext

`SparkContext` is the **original** entry point. It represents the connection
to the Spark cluster.

**What it does:**
- Creates RDDs (the old low-level API)
- Broadcasts variables
- Creates Accumulators
- Configures the cluster connection

**How to get it:**
```python
from pyspark import SparkContext, SparkConf

# Old way (Spark 1.x):
conf = SparkConf().setAppName("MyApp").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Modern way (Spark 2.x+):
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext   # get from session
```

**What SparkContext owns internally:**
- Connection to cluster manager
- Task scheduler
- DAG scheduler
- Block manager (tracks partition locations)

---

### SparkSession

`SparkSession` is the **modern unified entry point** (Spark 2.0+).

It wraps and supersedes SparkContext. With SparkSession, you can:
- Work with DataFrames and Datasets (the high-level API)
- Run Spark SQL queries
- Read from files (CSV, JSON, Parquet, ORC, Delta)
- Still access RDDs via `spark.sparkContext`

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Get the underlying SparkContext if you need it:
sc = spark.sparkContext
```

**`getOrCreate()` pattern:**
- If a SparkSession already exists in the JVM → returns it
- If not → creates a new one
- Prevents accidentally creating multiple sessions (only one active per JVM)

---

### SparkContext vs SparkSession — Side by Side

| Feature | SparkContext | SparkSession |
|---------|-------------|-------------|
| Introduced | Spark 1.0 (2014) | Spark 2.0 (2016) |
| API Level | Low-level (RDD) | High-level (DataFrame, SQL) |
| Create RDDs | `sc.parallelize()` | `spark.sparkContext.parallelize()` |
| Create DataFrames | Not directly | `spark.createDataFrame()` |
| Read files | Not directly | `spark.read.csv()`, `.parquet()` etc. |
| Run SQL | Not directly | `spark.sql("SELECT ...")` |
| Access from other | — | `spark.sparkContext` |
| Should you use? | Only for RDD work | Preferred for everything |

---

### Builder Pattern — SparkSession Configuration

```python
spark = SparkSession.builder \
    .appName("ETL Pipeline")               # Name shown in Spark UI
    .master("local[*]")                    # local mode or spark://host:7077
    .config("spark.driver.memory", "2g")   # Driver RAM
    .config("spark.executor.memory", "4g") # Executor RAM
    .config("spark.executor.cores", "4")   # Cores per executor
    .config("spark.sql.shuffle.partitions", "200")  # After shuffle, how many partitions
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .enableHiveSupport()                   # Enable Hive metastore (optional)
    .getOrCreate()
```

**For Delta Lake**, add:
```python
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

---

### Multiple SparkSessions (Spark 3.x)

Spark 3.x supports multiple sessions sharing one SparkContext:

```python
spark1 = SparkSession.builder.appName("App1").getOrCreate()
spark2 = spark1.newSession()  # new session, same underlying SparkContext

# Each session has its own SQL config and temp views
spark1.conf.set("spark.sql.shuffle.partitions", "50")
spark2.conf.set("spark.sql.shuffle.partitions", "200")

# Temp views are session-scoped
spark1.createDataFrame([("a",)], ["col"]).createOrReplaceTempView("t1")
spark2.sql("SELECT * FROM t1")  # ERROR — t1 not visible in spark2
```

---

## Interview Cheat Sheet

**Q: What is the difference between SparkContext and SparkSession?**
> SparkContext is the original Spark 1.x entry point for working with RDDs and
> connecting to the cluster. SparkSession (introduced in Spark 2.0) is the unified
> entry point that wraps SparkContext and adds support for DataFrames, Datasets, and
> Spark SQL. For modern Spark (2.x+) you always start with SparkSession and access
> SparkContext via `spark.sparkContext` when needed.

**Q: What is a Spark Job?**
> A Job is triggered every time you call an Action (collect, count, show, write, etc.).
> It represents the complete computation needed to satisfy that action. One Spark
> application can contain many jobs.

**Q: What is a Stage?**
> A Stage is a set of tasks that can be computed without shuffling data across
> the network. Stage boundaries occur at shuffle operations (groupBy, join, orderBy).
> A job with one groupBy will have two stages: one before and one after the shuffle.

**Q: What is a Task?**
> A Task is the smallest unit of work. One task processes one partition of data
> and runs on one executor core. The number of tasks in a stage equals the number
> of partitions in that stage.

**Q: How many parallel tasks can run?**
> Maximum parallel tasks = total executor cores.
> If you have 5 executors × 4 cores = 20 parallel tasks.
> If your stage has 100 partitions/tasks, they run in 5 waves of 20.

**Q: What does `getOrCreate()` do?**
> It returns the existing SparkSession if one already exists in the JVM, or creates
> a new one if not. This prevents accidentally spawning multiple SparkContexts in
> the same JVM (which would cause an error in Spark 1.x and is still discouraged).
