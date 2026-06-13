# Topic 4: DAG — Directed Acyclic Graph

> Phase 1 → Week 1 → Topic 4

---

## The GPS Navigation Analogy

Imagine you're planning a road trip. Before you start driving:
1. You open Google Maps
2. You enter all your stops: Home → Gas Station → Grocery → Friend's House → Work
3. Google Maps builds the entire **route plan** (DAG)
4. It finds the optimal path — maybe combines stops, avoids traffic
5. **Only then** do you start driving

You don't drive to each stop and re-plan from there. You plan the whole trip first.

Spark does the same: **plan the entire computation first, then execute optimally.**

This "plan" is the **DAG**.

---

## 1. What is a DAG?

**Directed Acyclic Graph** = a graph where:
- **Directed**: edges point in one direction (A → B means B depends on A)
- **Acyclic**: no cycles (you can't loop back — computation moves forward only)

In Spark:
- **Nodes** = RDD or DataFrame transformations
- **Edges** = data flows from one transformation to the next
- **Direction** = data flows forward (result of one step feeds the next)
- **Acyclic** = no transformation can depend on a future result of itself

---

## 2. How Spark Builds the DAG

Every time you write a transformation (like `.filter()`, `.groupBy()`, `.join()`),
Spark does NOT execute it. Instead it **adds a node to the DAG**.

```python
# Let's trace what DAG gets built:

df = spark.read.csv("sales.csv")
# DAG:  [ReadCSV]

df2 = df.filter(df.amount > 100)
# DAG:  [ReadCSV] → [Filter: amount > 100]

df3 = df2.withColumn("tax", df2.amount * 0.18)
# DAG:  [ReadCSV] → [Filter] → [WithColumn: tax]

df4 = df3.groupBy("region").sum("amount", "tax")
# DAG:  [ReadCSV] → [Filter] → [WithColumn] → [GroupBy+Sum]

# Still nothing has run. DAG is just a recipe.

df4.show()  # ← THIS triggers actual execution!
# Spark looks at DAG, optimizes it, splits into stages, creates tasks, runs them
```

---

## 3. DAG Visualization

```
                   ┌────────────────┐
                   │  Read CSV      │  ← Stage 0 start
                   └───────┬────────┘
                           │
                   ┌───────▼────────┐
                   │  Filter        │  ← narrow (same partition)
                   │  amount > 100  │
                   └───────┬────────┘
                           │
                   ┌───────▼────────┐
                   │  WithColumn    │  ← narrow (same partition)
                   │  tax = amt×0.18│
                   └───────┬────────┘
                           │
                    ════════════════    ← STAGE BOUNDARY (shuffle)
                           │
                   ┌───────▼────────┐
                   │  GroupBy+Sum   │  ← Stage 1 start
                   │  by region     │
                   └───────┬────────┘
                           │
                   ┌───────▼────────┐
                   │  show()        │  ← ACTION (triggers execution)
                   └────────────────┘
```

**Key points:**
- The DAG flows top-to-bottom (directed)
- Each node depends only on nodes above it (acyclic)
- Stage boundaries appear at wide transformations (shuffle)

---

## 4. Why DAG? The Catalyst Optimizer

When Spark builds the DAG, it doesn't execute it immediately. It passes it through
the **Catalyst Optimizer** — Spark's query optimization engine.

```
Your Code (Transformations)
         │
         ▼
   Unresolved Logical Plan
   (check column names exist)
         │
         ▼
   Resolved Logical Plan
   (schema verified)
         │
         ▼
   Optimized Logical Plan    ← Catalyst applies optimizations
   • Predicate Pushdown      (move filters earlier, closer to data source)
   • Column Pruning          (drop unused columns early)
   • Constant Folding        (pre-compute constants)
   • Join Reordering         (join smaller tables first)
         │
         ▼
   Physical Plan(s)          ← Multiple possible execution strategies
   (e.g. BroadcastHashJoin vs SortMergeJoin)
         │
         ▼
   Selected Physical Plan    ← Catalyst picks the cheapest
         │
         ▼
   RDD Execution             ← Code generation, Tungsten execution
```

### Predicate Pushdown Example

```python
# You write:
df.join(other_df, "id").filter(df.country == "India")

# Without optimization:
# 1. Read ALL of df
# 2. Read ALL of other_df
# 3. Join them (millions of rows)
# 4. Filter to India (keep small subset)

# With Catalyst Predicate Pushdown:
# 1. Filter df to India FIRST (much fewer rows)
# 2. Read ALL of other_df
# 3. Join (much smaller df × other_df)

# Same result, way faster!
```

This is something Spark does automatically because of the DAG — it can see
the whole plan before executing, and rearrange steps for efficiency.

### Column Pruning Example

```python
# You write:
df.select("name", "salary").filter(df.salary > 50000)

# The CSV has 20 columns. Catalyst knows you only need 'name' and 'salary'.
# It tells the CSV reader: "only read these 2 columns from disk"
# = 18 columns never read from disk = faster I/O
```

---

## 5. The Lineage Graph (Fault Tolerance via DAG)

The DAG also serves as Spark's **fault tolerance mechanism**.

In MapReduce: if a node fails → re-read from HDFS (data was written there).
In Spark: if an executor dies → re-compute from the DAG lineage.

```
RDD_A (original data from HDFS/S3)
   │
   ▼
RDD_B = RDD_A.filter(...)
   │
   ▼
RDD_C = RDD_B.map(...)     ← Executor holding RDD_C's partition dies!
   │
   ▼
RDD_D = RDD_C.groupBy(...)
```

Spark knows from the DAG:
- "RDD_C partition 3 is gone. But I know how to rebuild it:
   take RDD_A partition 3, apply filter, then map."
- Spark re-runs just those steps for just that partition.

This is called **lineage-based fault tolerance** — the DAG IS the recovery plan.

**Key difference from MapReduce fault tolerance:**
- MapReduce: replicate data to disk (3 copies) → recover by reading disk copy
- Spark: remember the computation (DAG) → recover by re-computing

---

## 6. How to See the DAG

### Method 1: Spark UI (Visual)
```python
df.groupBy("dept").count().show()
# Go to Spark UI → Jobs → click the job → "DAG Visualization"
# You'll see a visual graph of stages and dependencies
```

### Method 2: explain() in Code
```python
df.filter(df.salary > 50000).groupBy("dept").count().explain()
# Prints the physical execution plan (text version of what DAG will execute)

# For verbose output (shows all 4 plan stages):
df.filter(df.salary > 50000).groupBy("dept").count().explain(True)
```

### Method 3: explain(mode="formatted") — Spark 3.x
```python
df.groupBy("dept").count().explain(mode="formatted")
# Cleaner multi-line format showing:
# == Physical Plan ==
# == Analyzed Logical Plan ==
# == Optimized Logical Plan ==
# == Parsed Logical Plan ==
```

---

## 7. DAG Properties That Matter for Performance

### Wide vs Narrow Dependencies (Preview — full coverage in Phase 2)

**Narrow dependency**: Each parent partition feeds **one** child partition.
```
Partition 0 → Partition 0   (e.g., map, filter, select)
Partition 1 → Partition 1
Partition 2 → Partition 2
```
No shuffle. Same stage. Fast.

**Wide dependency**: Each parent partition can feed **many** child partitions.
```
Partition 0 →┬→ Partition 0
              ├→ Partition 1  (e.g., groupBy, join, orderBy)
              └→ Partition 2
Partition 1 →┬→ Partition 0
              └→ Partition 1
```
Requires shuffle. New stage. Expensive.

**Optimization goal**: Minimize wide dependencies (shuffles) in your DAG.

---

## 8. Interview Cheat Sheet

**Q: What is a DAG in Spark?**
> DAG stands for Directed Acyclic Graph. In Spark, it represents the logical
> execution plan of your computation — a graph where nodes are transformations
> and edges represent data flow between them. It's "directed" because data flows
> in one direction and "acyclic" because there are no circular dependencies.
> Spark builds the DAG lazily (before executing) and uses it to optimize the
> physical execution plan via the Catalyst Optimizer.

**Q: Why does Spark use a DAG instead of immediately executing each transformation?**
> Because seeing the complete computation plan allows Spark to:
> 1. Apply optimizations (predicate pushdown, column pruning, join reordering)
> 2. Minimize unnecessary work
> 3. Group transformations into stages intelligently
> 4. Provide lineage for fault tolerance without disk replication
> This is fundamentally different from MapReduce, which executes each step immediately.

**Q: What is the Catalyst Optimizer?**
> The Catalyst Optimizer is Spark's query optimization engine. It takes the logical
> plan (DAG) and applies rule-based and cost-based optimizations: predicate pushdown
> (filters applied close to data source), column pruning (unused columns dropped early),
> constant folding, and join strategy selection. It then generates an optimized physical
> plan for execution.

**Q: How does Spark recover from failures?**
> Via lineage (the DAG). When an executor fails and loses its partition data, Spark
> consults the DAG to find out how that partition was derived — then re-runs only the
> necessary transformations on the parent data to rebuild it. No disk checkpointing
> needed for intermediate data.

**Q: How many DAGs does a Spark application have?**
> One DAG per Job. Each Action (count, show, collect, write) triggers one Job,
> and each Job has its own DAG that gets converted into Stages and Tasks.

---

## 9. Summary

```
What DAG does:
1. Records your transformations without executing them (lazy)
2. Represents the full computation as a graph (plan)
3. Gets passed to Catalyst Optimizer for optimization
4. Gets split into Stages at shuffle boundaries
5. Stages get split into Tasks (one per partition)
6. Acts as lineage for fault tolerance

DAG flow:
  Code → Unresolved Plan → Resolved Plan → Optimized Plan → Physical Plan → RDD Code
```
