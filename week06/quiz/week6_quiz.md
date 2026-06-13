# Week 6 Quiz — Performance & Optimization: Memory

> Answer without looking at the notebooks. Check answers afterward.

---

## Section 1: Memory Model (10 points)

**Q1.** Draw the memory regions of a Spark executor with `executor.memory=8g`, `memory.fraction=0.6`, `memory.storageFraction=0.5`. Calculate the size of each region in MB.

**Q2.** What is the Unified Memory Manager and how does it differ from the static memory manager used before Spark 1.6?

**Q3.** Execution memory needs more space than available and storage memory is partially used. What happens?

**Q4.** You set `spark.executor.memory=16g` on a node that only has 16GB of RAM. Will this work? Why or why not?

**Q5.** What does `spark.executor.memoryOverhead` cover? What's its default value?

---

## Section 2: Executor Configuration (8 points)

**Q6.** You have a cluster with 6 nodes × 20 vCPU × 128GB RAM. Using the industry rule of 4 cores per executor and 1 core/1GB reserved per node, calculate the recommended `executor.instances`, `executor.cores`, `executor.memory`, and `shuffle.partitions`.

**Q7.** What's wrong with this configuration for a 5-node × 8-core × 32GB cluster?
```
--num-executors 2
--executor-cores 20
--executor-memory 75g
```

**Q8.** What is dynamic allocation? What additional service must be running for it to work correctly?

---

## Section 3: GC & Serialization (8 points)

**Q9.** What are "stop-the-world" GC pauses and why are they dangerous for Spark jobs?

**Q10.** How do you enable G1GC for executors in a SparkSession? Write the config key and value.

**Q11.** What is the difference between `MEMORY_ONLY` and `MEMORY_ONLY_SER` storage levels in terms of GC impact?

**Q12.** Why doesn't Kryo serialization improve DataFrame operation performance?

---

## Section 4: Spill & OOM (8 points)

**Q13.** A Spark UI stage shows: `Spill (Memory): 5.2 GB, Spill (Disk): 4.8 GB`. What do these numbers mean and what should you do?

**Q14.** Your job fails with `OutOfMemoryError` during a `BroadcastHashJoin`. What's the likely cause and how do you fix it?

**Q15.** List the 4-step OOM diagnosis workflow in order.

**Q16.** A job has 200 shuffle partitions. Stage shows one task with input 45GB and all others with input <100MB. What is this called and what are 2 ways to fix it?

---

## Section 5: Broadcast & Accumulators (6 points)

**Q17.** What's the difference between `sc.broadcast(my_dict)` and `F.broadcast(df)` in a join?

**Q18.** Why are accumulators unreliable in transformations but reliable in actions?

**Q19.** Write code that uses a broadcast variable to add a `continent` column to a DataFrame containing country names, without using a join.

---

## Answers

<details>
<summary>Click to reveal answers</summary>

**A1.**
```
Total heap = 8192 MB
Reserved   = 300 MB
Usable     = 8192 - 300 = 7892 MB
User Memory = 7892 × 0.4 = 3156.8 MB  (~3.1 GB)
Spark Pool  = 7892 × 0.6 = 4735.2 MB  (~4.6 GB)
  Storage   = 4735.2 × 0.5 = 2367.6 MB (~2.3 GB, initial — can borrow)
  Execution = 4735.2 × 0.5 = 2367.6 MB (~2.3 GB, initial — can borrow)
```

**A2.** Before 1.6, execution and storage memory were statically partitioned — a fixed fraction each. If execution was full but storage had spare space (or vice versa), that spare space was wasted. The Unified Memory Manager pools both into one shared region. Execution can evict cached storage partitions when it needs more space; storage can expand into idle execution memory. Dynamic = no wasted memory.

**A3.** Spark evicts cached data (LRU — least recently used partitions) from storage memory to free space for execution. The evicted partitions are dropped from cache and will be recomputed from scratch if accessed again. This is safe — slow, but no task failure.

**A4.** No, it will fail. `executor.memory=16g` means the JVM heap is 16GB. But the actual physical memory used is `executor.memory + memoryOverhead` = 16GB + max(1.6GB, 384MB) ≈ 17.6GB. The node only has 16GB — YARN/K8s will refuse to launch the container or kill it for exceeding the node's memory limit. Always leave room for overhead.

**A5.** `executor.memoryOverhead` covers non-heap memory: Python worker processes (PySpark), JVM native code (JNI), OS-level thread stacks and buffers, Arrow for Pandas UDFs. Default = `max(384MB, 0.1 × executor.memory)`. For PySpark jobs with heavy Pandas UDFs, increase it (e.g., 1-2GB).

**A6.**
```
Cores for Spark per node: 20 - 1 = 19
RAM for Spark per node:   128 - 1 = 127 GB
Executors per node:       19 // 4 = 4 (with 3 cores leftover)
Total executors:          4 × 6 = 24
Total cores:              24 × 4 = 96
Memory per executor:      127 / 4 = ~31.75 → 28g (×0.9 for overhead)
shuffle.partitions:       96 × 3 = 288 (or use 200-300 range)
```

**A7.** Three problems: (1) 20 cores per executor on an 8-core node — impossible, a single executor can't span multiple nodes; (2) 75GB per executor on a 32GB node — 2× the node's RAM; (3) only 2 executors for 5 nodes — 3 nodes completely idle. Correct: ~5 executors per node, 2 cores, ~12g memory = 25 total executors.

**A8.** Dynamic allocation scales executor count up when tasks queue up, and releases idle executors after a timeout. It requires an **External Shuffle Service** (or in Spark 3.2+, shuffle tracking) so released executors don't lose their shuffle data. The external service retains shuffle files independently of executor lifecycle. Config: `spark.dynamicAllocation.enabled=true`, `spark.shuffle.service.enabled=true`.

**A9.** "Stop-the-world" means the JVM halts ALL threads (including your task threads) while GC runs. During that pause, tasks make zero progress — they're completely frozen. If the pause is long enough (> `spark.executor.heartbeatInterval`, default 10s), the driver considers the executor dead and reschedules its tasks, wasting all completed work. Frequent pauses also reduce throughput significantly.

**A10.** `spark.executor.extraJavaOptions = -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35`

**A11.** `MEMORY_ONLY` stores deserialized Java objects on the heap — many objects means more GC work (all those pointers to traverse). `MEMORY_ONLY_SER` stores compact byte arrays — fewer objects on heap means less GC pressure and shorter GC cycles. Tradeoff: deserializing on each access adds CPU cost. Use SER when GC pauses are a problem and access is sequential (not random).

**A12.** DataFrame operations don't use Java serialization or Kryo at all — they use Tungsten's binary format (compact, columnar, off-heap). Kryo only affects RDD operations, broadcast variables, and closures (JVM side). DataFrame operators are compiled down to efficient JVM bytecode by Tungsten's whole-stage code generation.

**A13.** `Spill (Memory) = 5.2GB`: the total size of data that was in memory before it had to be spilled (i.e., the spilled data was 5.2GB in deserialized form). `Spill (Disk) = 4.8GB`: the actual bytes written to disk (typically smaller due to serialization compression). Fix: increase `spark.sql.shuffle.partitions` to create smaller partitions, or increase executor memory.

**A14.** The broadcast table is too large to fit in executor heap memory. The broadcast data is replicated to every executor and held entirely in memory — there's no spill mechanism. Fix: (1) Remove the `broadcast()` hint and let Spark use SortMergeJoin (which can spill); (2) Filter the broadcast table to reduce its size; (3) Lower `spark.sql.autoBroadcastJoinThreshold` to prevent auto-broadcasting; (4) Increase executor memory.

**A15.**
1. Identify WHERE: driver OOM vs executor OOM (check logs and Spark UI Executors tab)
2. Driver path: check for `collect()` on large data, large broadcast creation → fix or increase `driver.memory`
3. Executor path: check for skew (one huge task), large broadcast joins, spill metrics
4. Try cheapest fix first: more partitions → fix skew → remove broadcast hint → increase memory

**A16.** Data skew — one key has vastly more data than others, so one partition/task gets almost all the data. Fixes: (1) Salting — add a random suffix to the skewed key to spread it across multiple partitions; (2) AQE skew join — enable `spark.sql.adaptive.skewJoin.enabled=true` and Spark will automatically split the skewed partition at runtime.

**A17.** `sc.broadcast(my_dict)` creates a Spark broadcast variable — a Python dict (or any serializable object) that's sent to each executor once via an efficient peer-to-peer protocol and cached there. It's accessed in RDD operations or UDFs via `.value`. `F.broadcast(df)` is a query hint to Catalyst telling it to use BroadcastHashJoin for a DataFrame join — it broadcasts the DataFrame's data to all executors during join execution. Different mechanism, different use case.

**A18.** In transformations (map, filter), Spark may re-execute a task multiple times: once for fault tolerance retries, once for speculative execution, and once if the same transformation is called multiple times (due to lazy evaluation and DAG re-execution). Each execution adds to the accumulator. Actions have different semantics — each task in an action runs exactly once per successful job stage (with retries only on failure). Even then, task retries can double-count, but at least you won't accumulate on each `count()` call.

**A19.**
```python
continent_map = {"India": "Asia", "USA": "America", "UK": "Europe", ...}
bc = sc.broadcast(continent_map)

@udf(StringType())
def get_continent(country):
    return bc.value.get(country, "Other") if country else "Unknown"

df.withColumn("continent", get_continent(F.col("country")))
bc.unpersist()
```

</details>
