# Topic 2: Spark Architecture — Driver, Cluster Manager, Workers, Executors

> Phase 1 → Week 1 → Topic 2

---

## The Restaurant Analogy (Read This First)

Think of a Spark cluster like a **restaurant kitchen**:

| Kitchen Component | Spark Component | Role |
|-------------------|-----------------|------|
| Head Chef (you) | **Driver** | Decides what to cook, plans the recipe, coordinates everything |
| Restaurant Manager | **Cluster Manager** | Manages the kitchen resources (who is free, who is busy) |
| Cooking stations | **Worker Nodes** | Physical machines where cooking happens |
| Individual cooks | **Executors** | The actual processes doing the cooking on each station |
| Tasks on a recipe card | **Tasks** | Individual steps ("chop onions", "boil water") |

The head chef doesn't chop vegetables — they coordinate. The executors do the real work.

---

## 1. The Big Picture

```
                         ┌──────────────────────────────┐
                         │         DRIVER NODE          │
                         │  ┌────────────────────────┐  │
                         │  │     Your PySpark Code   │  │
                         │  │    (SparkSession)       │  │
                         │  └──────────┬─────────────┘  │
                         │             │                 │
                         │  ┌──────────▼─────────────┐  │
                         │  │      SparkContext        │  │
                         │  │   (DAGScheduler +       │  │
                         │  │    TaskScheduler)        │  │
                         │  └──────────┬─────────────┘  │
                         └────────────┼─────────────────┘
                                      │  (sends tasks)
                         ┌────────────▼─────────────────┐
                         │      CLUSTER MANAGER          │
                         │  (YARN / Kubernetes /         │
                         │   Standalone / Mesos)         │
                         └──┬──────────────────────┬─────┘
                            │                      │
              ┌─────────────▼──┐            ┌──────▼────────────┐
              │  WORKER NODE 1  │            │   WORKER NODE 2   │
              │  ┌───────────┐  │            │  ┌───────────┐    │
              │  │ Executor  │  │            │  │ Executor  │    │
              │  │ ┌───┐┌───┐│  │            │  │ ┌───┐┌───┐│   │
              │  │ │T1 ││T2 ││  │            │  │ │T3 ││T4 ││   │
              │  │ └───┘└───┘│  │            │  │ └───┘└───┘│   │
              │  │  (RAM+CPU) │  │            │  │  (RAM+CPU) │   │
              │  └───────────┘  │            │  └───────────┘    │
              └─────────────────┘            └───────────────────┘
```

---

## 2. Component Deep Dives

### 2.1 Driver

The **Driver** is the brain of your Spark application.

**What it does:**
- Runs your `main()` program / PySpark script
- Creates the `SparkSession` and `SparkContext`
- Converts your code into a **DAG** (Directed Acyclic Graph) of stages
- Schedules tasks and sends them to executors
- Collects results from executors when you call actions like `.collect()`
- Tracks job progress and reports failures

**What it does NOT do:**
- Does NOT process the actual data
- Does NOT store data (except when you call `.collect()` — data comes back to driver)

**Where it runs:**
- **Client mode**: Driver runs on your laptop/submission machine → good for interactive use, bad for stability in production
- **Cluster mode**: Driver runs on one of the worker nodes → better for production (doesn't die if your laptop disconnects)

**Key internal components:**
- `DAGScheduler`: Converts logical plan (DAG) into physical stages
- `TaskScheduler`: Assigns tasks to executors, tracks their status
- `BlockManager` (driver side): Tracks where data partitions live

**Driver memory setting:**
```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
```

**Important**: If you call `.collect()` on a huge dataset, ALL data comes to driver RAM.
This is a common cause of driver OOM (Out of Memory) errors in production.

---

### 2.2 Cluster Manager

The **Cluster Manager** is the resource broker. It doesn't know anything about Spark —
it just manages machine resources (CPU cores, RAM).

**Its job:**
- Allocate resources (cores + memory) to Spark applications
- Launch executors on worker nodes
- Monitor health of nodes

**Available Cluster Managers:**

| Cluster Manager | When to Use | Notes |
|-----------------|-------------|-------|
| **Standalone** | Learning / small clusters | Built into Spark. Simple. |
| **YARN** | Hadoop clusters | Most common in on-prem deployments |
| **Kubernetes** | Modern cloud-native | Growing standard. Docker + K8s. |
| **Mesos** | Legacy | Largely replaced by K8s |
| **Databricks** | Managed cloud (AWS/Azure/GCP) | Proprietary, hides cluster manager |
| **EMR** | AWS managed | Uses YARN under the hood |

Spark talks to the cluster manager via its **ClusterManager interface** —
Spark doesn't care which one you use.

---

### 2.3 Worker Nodes

A **Worker Node** is simply a **physical or virtual machine** in the cluster.

**What it contains:**
- One or more **Executors** (JVM processes)
- CPU cores available for task execution
- RAM available for data storage and processing
- Disk for spilling overflow data

**The Worker Node itself doesn't "do" Spark work.** It just hosts executors.

In Standalone mode, a `Worker daemon` runs on each node and registers with the Master.
It receives commands from the Master to launch Executor processes.

---

### 2.4 Executor

The **Executor** is a **JVM process** running on a Worker Node. This is where
data is actually processed.

**What it does:**
- Runs **Tasks** assigned by the Driver's TaskScheduler
- Stores data in memory (RDD partitions, DataFrame partitions, cached data)
- Reports task status and metrics back to the Driver
- Each executor runs multiple tasks concurrently (one per core)

**Executor sizing:**
```
Executor Memory: spark.executor.memory  (default: 1g)
Executor Cores : spark.executor.cores   (default: 1)
Num Executors  : spark.executor.instances

Example: 3 executors × 4 cores × 4GB each
  = 12 parallel tasks
  = 12 GB executor memory total
```

**Memory breakdown inside an executor:**
```
┌──────────────────────────── Executor Memory ───────────────────────────────┐
│                                                                              │
│  ┌──────────────┐  ┌──────────────────────────────────────────────────┐    │
│  │  Reserved    │  │              Usable Memory                        │    │
│  │  Memory      │  │  ┌──────────────────────┐  ┌──────────────────┐  │    │
│  │  (~300 MB)   │  │  │ Storage Memory        │  │ Execution Memory  │  │    │
│  │              │  │  │ (cached RDDs,         │  │ (shuffles, joins, │  │    │
│  │              │  │  │  DataFrames)          │  │  sorts, aggs)    │  │    │
│  │              │  │  │   60% of usable       │  │  40% of usable   │  │    │
│  └──────────────┘  │  └──────────────────────┘  └──────────────────┘  │    │
│                    └──────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────────┘
  (Note: With Unified Memory Manager in Spark 1.6+, storage and execution
   can borrow from each other dynamically)
```

---

### 2.5 Tasks

A **Task** is the **smallest unit of work** in Spark.

- One task processes **one partition** of data
- Tasks are sent from Driver → Executor as serialized code
- Tasks run in parallel (one per executor core)

If your data has 100 partitions → Spark creates 100 tasks for that stage.

---

## 3. How a Spark Job Actually Runs — Step by Step

```
You write:
    df = spark.read.csv("data.csv")
    result = df.filter(df.age > 25).groupBy("city").count()
    result.show()           ← this is an ACTION, triggers execution

Step 1: Driver receives the action call (.show())
Step 2: Driver's DAGScheduler builds a DAG of stages
Step 3: DAGScheduler splits DAG into stages at shuffle boundaries
Step 4: TaskScheduler requests resources from Cluster Manager
Step 5: Cluster Manager tells Worker Nodes to launch Executors
Step 6: Executors register with the Driver ("I'm ready")
Step 7: Driver sends Tasks to Executors (one task per partition)
Step 8: Executors run tasks, read data, process it
Step 9: If shuffle needed: Executors write shuffle data, other executors read it
Step 10: Executors send results/status back to Driver
Step 11: Driver shows you the result (.show())
```

---

## 4. Deploy Modes

### Client Mode (default for `spark-submit` with `--deploy-mode client`)
```
Your Machine
   ├── Driver (your script runs here)
   └── talks to Cluster Manager → Executors on remote nodes

Pros: Easy to debug, see logs directly, good for interactive Jupyter
Cons: If your machine dies → job dies. Not for production.
```

### Cluster Mode (`--deploy-mode cluster`)
```
Your Machine
   └── submits job to Cluster Manager

Cluster Manager picks a Worker Node to run the Driver
   ├── Driver (running on a worker node)
   └── Executors (on other worker nodes)

Pros: Job survives if your laptop disconnects. Better for production.
Cons: Harder to debug, logs are on the cluster.
```

---

## 5. Local Mode (for Learning & Testing)

```python
spark = SparkSession.builder \
    .master("local[*]") \  # use all CPU cores on your machine
    .getOrCreate()
```

In **local mode**:
- Driver and Executor run in the **same JVM** on your machine
- No cluster manager needed
- `local[1]` = 1 thread, `local[4]` = 4 threads, `local[*]` = all cores
- Perfect for development and learning
- Our Docker setup runs in local mode by default

---

## 6. Spark Architecture: Key Differences from MapReduce

| Aspect | MapReduce | Spark |
|--------|-----------|-------|
| Processing model | Disk-based | Memory-first, spills to disk |
| Multi-step pipelines | Separate jobs, disk between each | Single DAG, in-memory |
| Data model | Key-Value pairs only | RDDs, DataFrames, Datasets |
| Cluster manager | YARN (built-in) | YARN, K8s, Standalone, Mesos |
| Fault tolerance | Re-read from HDFS | Re-compute from lineage |
| Latency | Minutes per job | Seconds per job |

---

## 7. Interview Cheat Sheet

**Q: What is the Spark Driver?**
> The Driver is the master process that runs your Spark application code.
> It creates the SparkContext, builds the DAG, schedules tasks via the
> TaskScheduler, and coordinates execution. It does NOT process data itself.
> All results from `.collect()` come back to the Driver's memory.

**Q: What is an Executor?**
> An Executor is a JVM process on a Worker Node that runs Tasks assigned by the Driver.
> Each executor has a fixed amount of RAM and CPU cores. It stores data in memory
> (cached RDDs/DataFrames), runs tasks concurrently (one per core), and reports
> back to the Driver.

**Q: What is the Cluster Manager?**
> The Cluster Manager is a resource allocator that sits between the Driver and Worker Nodes.
> It allocates CPU and RAM to Spark applications and launches executors. Spark supports
> YARN, Kubernetes, Standalone, and Mesos as cluster managers.

**Q: What's the difference between Worker Node and Executor?**
> A Worker Node is the physical/virtual machine. An Executor is a JVM process
> running ON that machine. One Worker Node can have one or more Executors.
> Think of Worker Node = the building, Executor = an employee working in it.

**Q: What happens if an Executor dies?**
> The Driver detects the failure (no heartbeat). It marks the tasks that were
> running on that executor as failed. Those tasks are rescheduled on other executors.
> The data that was in memory on the dead executor is recomputed from the lineage
> (the original data source + transformations). This is Spark's fault tolerance model.

**Q: What happens if the Driver dies?**
> The entire application fails. This is why Cluster Mode is preferred for production —
> the Driver runs on a resilient node managed by the cluster manager.

**Q: How many tasks run in parallel?**
> `total parallel tasks = number of executor cores across all executors`
> Example: 4 executors × 4 cores = 16 tasks running in parallel at any given time.

---

## 8. Summary Diagram

```
YOUR CODE (.py file)
    │
    ▼
DRIVER (SparkContext / SparkSession)
    ├── Builds DAG
    ├── Splits into Stages
    ├── Creates Tasks
    └── Sends to →

CLUSTER MANAGER (YARN / K8s / Standalone)
    └── Launches Executors on →

WORKER NODES (machines with CPU + RAM + Disk)
    └── Run EXECUTORS (JVM processes)
            └── Run TASKS (one per partition, one per core)
                    └── Read data, transform it, return results
```
