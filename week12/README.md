# Week 12: Phase 7 — Advanced & Master Level

> **Phase 7 → Week 12**
>
> Prerequisites: All previous weeks (Phases 1-6)

---

## What You'll Learn This Week

Week 12 ties everything together. You go from "knows Spark" to "production-ready data engineer." This week covers cluster sizing, advanced join algorithms, vectorized Python with Arrow, cloud deployment on EMR/Databricks/Glue, and a comprehensive interview preparation masterclass.

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [Cluster Configuration](01_cluster_configuration.ipynb) | Memory model, executor sizing, dynamic allocation, OOM diagnosis |
| 02 | [Advanced Joins](02_advanced_joins.ipynb) | BHJ vs SMJ, join hints, skew handling, salting, Bloom filters |
| 03 | [Pandas UDFs & Arrow](03_pandas_udfs_arrow.ipynb) | Scalar/iterator/applyInPandas UDFs, Arrow optimization |
| 04 | [Cloud Platforms](04_cloud_platforms.ipynb) | EMR, Glue, Databricks, S3 patterns, cost optimization |
| 05 | [Interview Masterclass](05_interview_masterclass.ipynb) | Top 20 Q&A, live coding practice, system design, common mistakes |

---

## Key Rules to Remember

### Cluster Sizing
- Target 5 cores per executor (HDFS/S3 throughput sweet spot)
- Leave 1 core + 1 GB RAM per node for OS/YARN daemons
- `memoryOverhead` = max(executor_memory × 0.1, 384 MB) — for Python workers and JVM overhead
- Usable memory = `(executor.memory - 300 MB) × memory.fraction` (default 0.6)
- Execution and storage memory are unified — they can borrow from each other
- `shuffle.partitions`: target 2-3× total executor cores, or use AQE with a high ceiling

### Dynamic Allocation
- Enable with `spark.dynamicAllocation.enabled=true`
- Requires external shuffle service or `shuffleTracking.enabled=true` (Spark 3.x)
- Do NOT use for streaming (need stable partition assignment)
- Best for: shared clusters, variable-workload batch jobs

### Advanced Joins
- BHJ (Broadcast Hash Join): small table < 10 MB threshold → no shuffle → fastest
- SMJ (Sort-Merge Join): both tables large → two shuffles → correct but expensive
- Force BHJ: `df.join(F.broadcast(small_df), "key")`
- AQE auto-switches SMJ → BHJ if one side is smaller than expected at runtime
- AQE skew join: automatically splits large partitions
- Salting: manual skew fix — add random suffix + replicate small side
- Bloom Filter (Spark 3.3+): pre-filter large table before shuffle using small side's keys

### Pandas UDFs
- Regular UDF: row-by-row pickle serialization — avoid for math/string ops
- Scalar Pandas UDF: batch via Arrow → 2-10× faster than regular UDFs
- Iterator Pandas UDF: load model once per executor, apply to all batches
- `applyInPandas`: entire group as pandas DataFrame → per-group normalization, inference
- Always prefer built-in `F.*` functions — no JVM↔Python boundary at all
- Arrow (`spark.sql.execution.arrow.pyspark.enabled=true`): also speeds up `toPandas()` and `createDataFrame()`

### Cloud Platforms
- EMR: AWS-native, Spot instances (60-80% savings), tight IAM + Glue Catalog integration
- Databricks: best-in-class managed Spark, Delta built-in, collaborative notebooks, Unity Catalog
- Glue: serverless ETL, no cluster management, Glue Catalog + Athena integration, limited tuning
- S3: always use `s3a://`, use Parquet + Snappy, avoid many small files
- Checkpoint on S3/HDFS — never local disk in production (pod restarts will lose it)

---

## Interview Questions Covered

1. What is lazy evaluation?
2. Difference between repartition and coalesce?
3. When does a shuffle occur?
4. Difference between cache() and persist()?
5. What is data skew and how do you fix it?
6. What is AQE and its three main features?
7. When does Spark choose Broadcast Hash Join?
8. What is a Bloom Filter join?
9. What is the Spark memory model (execution vs storage memory)?
10. How do you size executors for a cluster?
11. What is a Pandas UDF vs regular UDF?
12. When to use `applyInPandas`?
13. What is the difference between Databricks and EMR?
14. What is AWS Glue and when to use it?
15. What is the `s3a://` vs `s3://` difference?
16. How do you diagnose OOM in Spark?
17. What are the most common Spark coding mistakes?
18. How would you design a real-time analytics platform end-to-end?
19. What is the Medallion Architecture end-to-end data flow?
20. What makes a Spark pipeline production-ready?

---

## Congratulations — Course Complete!

You have covered all 7 phases:

| Phase | Weeks | Topics |
|-------|-------|--------|
| 1 — Foundation | Week 1 | Architecture, DAG, lazy evaluation, RDD lineage |
| 2 — RDDs | Week 2 | Transformations, actions, pair RDDs, shuffle internals |
| 3 — DataFrames | Weeks 3-5 | Schema, joins, window functions, Spark SQL, UDFs |
| 4 — Performance | Weeks 6-7 | Partitioning, caching, AQE, Spark UI, Catalyst |
| 5 — ETL | Weeks 8-9 | Reading/writing, data quality, Delta Lake, Medallion |
| 6 — Streaming | Weeks 10-11 | Structured Streaming, Kafka, exactly-once, windows |
| 7 — Advanced | Week 12 | Cluster config, advanced joins, Pandas UDFs, cloud |

---

## How to Run

```bash
docker-compose up -d
# Open http://localhost:8888
# Navigate to week12/ and run notebooks in order
```

> **Interview tip:** After completing Week 12, do the Interview Masterclass exercises in notebook 05 from scratch — no copy-paste. That's the real test.
