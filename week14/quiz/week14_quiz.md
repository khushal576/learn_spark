# Week 14 Quiz — Orchestration & AWS Integration

> Test yourself after completing all 5 notebooks. No notes.

---

## Section A: Apache Airflow

**Q1.** What is the difference between `execution_date` and the actual wall-clock time a DAG runs? Why does Airflow use `execution_date` instead of `now()`?

<details><summary>Answer</summary>

`execution_date` is the logical date for a DAG run — it represents the START of the time period being processed, not when the job actually runs. A daily DAG with `execution_date=2024-01-15` runs on January 16th (Airflow runs after the interval closes). `ds` = `'2024-01-15'`.

Airflow uses logical dates so that: (1) backfill is deterministic — re-running a historical date produces the same result, (2) parametric queries (`WHERE date = '{{ ds }}'`) are self-contained, (3) task dependencies across DAGs can be expressed as "did the Jan 15 run of DAG X succeed?" regardless of wall time.
</details>

---

**Q2.** What is `catchup=False` and why is it critical in production? Give a scenario where you'd want `catchup=True`.

<details><summary>Answer</summary>

`catchup=False`: when a DAG is activated (or unpaused), Airflow only schedules the current interval — it does NOT backfill all missed runs since `start_date`. Critical in production: a DAG with `start_date=2020-01-01` and `catchup=True` would immediately try to run 4 years of daily jobs in parallel, overwhelming the cluster and your data.

`catchup=True` is useful when: (1) you're intentionally bootstrapping historical data (first-time load of a new pipeline), (2) you need to reprocess historical periods after a bug fix, (3) the pipeline is purely additive and re-running old periods is safe.
</details>

---

**Q3.** A Sensor uses `mode='poke'` with `poke_interval=30` and `timeout=3600`. Your Airflow cluster has 8 worker slots and 5 different DAGs are waiting on sensors simultaneously. What goes wrong?

<details><summary>Answer</summary>

In `poke` mode, the sensor holds its worker slot the entire time it's waiting. With 5 sensors in poke mode across 8 total slots, you've consumed 5/8 slots just for waiting — leaving only 3 slots for actual work. If more sensors are triggered, they queue up. Meanwhile, the sensors aren't doing anything useful — just sleeping between checks.

Fix: use `mode='reschedule'`. The sensor releases its worker slot between checks and only re-acquires it briefly each `poke_interval`. All 8 slots are available for real tasks while sensors wait.
</details>

---

**Q4.** What is XCom? What are its limits and what should you pass through it vs S3?

<details><summary>Answer</summary>

XCom (cross-communication) stores key-value pairs in Airflow's metadata DB (PostgreSQL/MySQL), accessible between tasks in the same DAG run.

**Pass via XCom**: cluster IDs, S3 paths, row counts, status codes, small config dicts (< a few KB).

**Pass via S3 (NOT XCom)**: DataFrames, large result sets, file contents, arrays with millions of elements. Write to S3 in task A, push the S3 path as XCom, task B reads from S3 using the path.

Limit: XCom serializes to Airflow's DB. Large values slow the scheduler, hit DB size limits, and cause memory issues. PostgreSQL has no hard XCom size limit, but best practice is < 48 KB. Some backends (SQLite) have 1 MB limit.
</details>

---

**Q5.** Write the four Airflow tasks needed to run a Spark job on an ephemeral EMR cluster. What `trigger_rule` must the terminate task use and why?

<details><summary>Answer</summary>

```python
create  = EmrCreateJobFlowOperator(task_id='create_cluster', ...)
submit  = EmrAddStepsOperator(task_id='add_steps', job_flow_id=cluster_id_xcom, ...)
wait    = EmrStepSensor(task_id='wait_steps', job_flow_id=cluster_id_xcom, ...)
destroy = EmrTerminateJobFlowOperator(
    task_id='terminate_cluster',
    job_flow_id=cluster_id_xcom,
    trigger_rule='all_done'    # ← KEY
)
create >> submit >> wait >> destroy
```

`trigger_rule='all_done'`: the terminate task runs regardless of whether upstream tasks succeeded or failed. Without this, if `wait_steps` fails (step failed on EMR), the default `all_success` rule would skip `terminate_cluster`, leaving a running EMR cluster that accumulates cost indefinitely.
</details>

---

## Section B: S3 Pipeline Patterns

**Q6.** A Spark streaming job writes micro-batch results to S3 every 30 seconds, producing 2 files per batch. After 24 hours, there are ~5,760 small files. What are the consequences and how do you fix it?

<details><summary>Answer</summary>

Consequences: (1) S3 LIST calls are O(files) — slow and expensive for the next reader, (2) each file requires separate open/close/read footer overhead in Spark, (3) the next Spark job has thousands of partitions (one per file) — more tasks than executors, high task scheduling overhead.

Fixes:
1. **Immediate**: compaction job: `spark.read.parquet(path).coalesce(N).write.mode("overwrite").parquet(path)` — run daily
2. **At write time**: use `foreachBatch` with `coalesce()` before writing each micro-batch
3. **Delta Lake OPTIMIZE**: `OPTIMIZE delta.path` — automatically compacts and can be scheduled
4. **Partition by date**: `partitionBy("date")` + daily compaction per partition keeps files bounded
</details>

---

**Q7.** What is `partitionOverwriteMode=dynamic`? How does it differ from plain `mode=overwrite`? Give a scenario where dynamic is critical.

<details><summary>Answer</summary>

`mode=overwrite` (default static): replaces the ENTIRE output path. Writing `date=2024-01-16` data overwrites everything, including `date=2024-01-15` already there.

`partitionOverwriteMode=dynamic`: only overwrites the partitions present in the NEW DataFrame. Writing data for `date=2024-01-16` only touches `date=2024-01-16/` — `date=2024-01-15/` is untouched.

Critical scenario: incremental daily job where re-running Jan 16 should only update Jan 16 data without destroying Jan 15 data. Without dynamic mode, a re-run with one day's data wipes the whole table.

Enable:
```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("date").parquet(path)
```
</details>

---

**Q8.** What is the S3 magic committer? What problem does it solve and how do you enable it?

<details><summary>Answer</summary>

Problem: S3 has no atomic rename (unlike HDFS). Standard Spark writes temp files with `_temporary/` prefix, then does `S3 COPY` + `S3 DELETE` to "rename" them. This is: (1) slow (double the data written), (2) not atomic (readers can see partial data during rename), (3) expensive (PUT + COPY + DELETE API calls).

Magic committer: instead of writing to temp and renaming, it writes final files directly using S3's multipart upload with a "magic" key that makes them invisible until committed. Commit is a metadata-only operation — atomic and cheap.

Enable:
```python
spark.conf.set("spark.hadoop.fs.s3a.committer.name", "magic")
spark.conf.set("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
```
</details>

---

## Section C: Monitoring

**Q9.** List 5 Spark UI metrics that should be checked for every production job run, and what each signals.

<details><summary>Answer</summary>

1. **Task duration distribution** (stage view): uniform = healthy; 1-2 tasks 10× longer = data skew
2. **GC time** (executor tab): > 10% of task time = memory pressure; reduce partition size or increase executor memory
3. **Shuffle read/write bytes** (stages): very high = too many wide transforms; consider broadcast joins or fewer shuffles
4. **Spill to disk** (stage metrics): any spill = executor memory too small for shuffle; increase `spark.executor.memory` or `shuffle.partitions`
5. **Executor lost events** (event log): node failure or OOM kill by OS; increase `memoryOverhead`, check yarn logs for OOM killer
</details>

---

**Q10.** What is data freshness monitoring? Write the SQL/code pattern for it and what SLAs are reasonable for Bronze/Silver/Gold layers.

<details><summary>Answer</summary>

Data freshness = `NOW() - MAX(event_timestamp)` in the output table. Measures how stale the data is.

```python
def check_freshness(df, ts_col, max_lag_hours, layer):
    max_ts = df.agg(F.max(ts_col)).collect()[0][0]
    lag_hours = (datetime.now() - max_ts).total_seconds() / 3600
    if lag_hours > max_lag_hours:
        raise ValueError(f"{layer} freshness violated: lag={lag_hours:.1f}h > {max_lag_hours}h")
```

Reasonable SLAs:
- **Bronze**: ≤ 15 minutes (near-real-time ingest from Kafka/S3 events)
- **Silver**: ≤ 1 hour (batch processing runs frequently)
- **Gold**: ≤ 2 hours after the data window closes (e.g., by 8am for previous day's Gold)
</details>

---

## Section D: End-to-End Architecture

**Q11.** Walk through a complete daily batch ETL pipeline on AWS from source to BI tool. Include all services and their roles.

<details><summary>Answer</summary>

1. **Ingest**: Source (RDS/Kafka/SFTP) → DMS/Kafka Connect → S3 Bronze (raw Parquet)
2. **Trigger**: S3 event → SQS → Lambda (or Airflow S3KeySensor) → start Airflow DAG run
3. **Orchestrate**: Airflow creates ephemeral EMR cluster (On-Demand master+core, Spot task nodes)
4. **Bronze job**: read S3 raw → add `_ingested_at` metadata → write to S3 Bronze (partitioned by date)
5. **Silver job**: deduplicate → validate → cast types → write to S3 Silver (Delta Lake, idempotent MERGE)
6. **Gold job**: aggregate → write to S3 Gold (Delta Lake) → run DQ + freshness checks
7. **Catalog**: Glue Crawler updates Glue Data Catalog with new partitions
8. **Serve**: Athena queries S3 via Glue Catalog; Redshift Spectrum for heavier SQL; QuickSight for dashboards
9. **Terminate**: Airflow terminates EMR cluster (`trigger_rule=all_done`)
10. **Monitor**: CloudWatch alarms on job duration, DQ violations, freshness lag; PagerDuty for P1
</details>

---

**Q12.** How do you handle a pipeline failure at the Silver step where Bronze ran successfully? Describe both the immediate recovery and the longer-term architectural change to make partial re-runs easier.

<details><summary>Answer</summary>

**Immediate recovery**:
1. Check EMR step logs in Airflow → identify root cause (OOM? Bad data? Schema change?)
2. Fix the issue (increase memory / fix data quality filter / update schema)
3. Clear only the Silver and Gold tasks in Airflow (`Clear` with `downstream=True`)
4. Airflow re-runs from Silver, Bronze is skipped (already succeeded)
5. Bronze output is idempotent (same files), so re-reading is safe

**Architectural improvement**: use `partitionOverwriteMode=dynamic` + date-partitioned writes for all layers. Then: (1) each layer's output is its own independent artifact keyed by date, (2) re-running Silver for `date=2024-01-15` only overwrites that date partition, (3) add a `layer_success_flag` file per partition (`date=2024-01-15/_SUCCESS`) — Airflow checks the flag before re-running to skip already-complete partitions.
</details>
