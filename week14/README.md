# Week 14: Phase 7 — Orchestration & AWS Integration

> **Phase 7 → Week 14**
>
> Prerequisites: Week 13 (Delta Lake advanced, Databricks deep dive, EMR, Glue, Lakehouse)

---

## What You'll Learn This Week

Week 14 covers Apache Airflow from scratch through production-grade EMR/Glue orchestration, S3 pipeline patterns (compaction, event-driven, idempotent writes), monitoring and observability, and a complete end-to-end AWS pipeline reference architecture.

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [Airflow Introduction](01_airflow_introduction.ipynb) | DAG structure, operators, sensors, hooks, XComs, SLAs, branching |
| 02 | [Airflow + Spark Operators](02_airflow_spark_operators.ipynb) | EMR lifecycle operators, GlueJobOperator, SparkSubmitOperator, connections |
| 03 | [S3 Pipeline Patterns](03_s3_pipeline_patterns.ipynb) | Small file compaction, partitioning strategy, event-driven triggers, idempotent writes |
| 04 | [Pipeline Monitoring](04_pipeline_monitoring.ipynb) | DQ metrics framework, freshness monitoring, Spark History Server, alerting runbooks |
| 05 | [End-to-End AWS Pipeline](05_end_to_end_aws_pipeline.ipynb) | Full architecture, complete DAG, production Spark job template, cost estimation |

---

## Key Rules to Remember

### Airflow Core
- DAG = Python file defining a pipeline; `schedule_interval` = cron string or timedelta
- `catchup=False`: only run current interval, skip historical backfill (always use in prod)
- `execution_date`: logical date for the run (not wall clock); `ds` = `YYYY-MM-DD` string
- Sensor `mode=reschedule`: release worker slot while waiting (use this, not `poke`, for long waits)
- XCom: pass small values (IDs, paths) between tasks; NEVER push large DataFrames
- `trigger_rule='all_done'`: use on the terminate cluster task so it runs even after failures

### EMR Orchestration
- Four-step pattern: `CreateCluster` → `AddSteps` → `EmrStepSensor` → `TerminateCluster`
- Always `trigger_rule='all_done'` on terminate — don't leak running clusters on failure
- XCom passes cluster ID between tasks: `ti.xcom_pull('create_cluster', key='return_value')`
- Sensor `mode=reschedule` + `poke_interval=60` for step sensors (EMR steps run for minutes)

### S3 Pipeline Patterns
- Small file problem: thousands of 1-10 MB files → slow LIST + open/close overhead → compaction
- Compaction: read all → `coalesce(N)` → write overwrite (run weekly or trigger on file count)
- `partitionOverwriteMode=dynamic`: overwrite only affected partitions (idempotent incremental)
- S3 event-driven: S3 → SQS → Lambda → trigger Glue/EMR (< 1s latency vs polling's N-second lag)
- Always `s3a://` (not `s3://` or `s3n://`); enable magic committer for atomic writes

### Monitoring
- Three layers: infrastructure (CloudWatch), job-level (Airflow/Spark UI), data-level (custom DQ)
- Data freshness: `MAX(event_ts) - NOW()` — alert if lag exceeds SLA
- Spark UI signals: straggler tasks (skew), GC > 10% (memory pressure), shuffle spill (OOM risk)
- Every alert must have a runbook link — otherwise it gets ignored
- Publish DQ metrics to S3/CloudWatch after each layer; Airflow DQ task reads and validates

---

## Interview Questions Covered

1. What is Apache Airflow and what problems does it solve vs cron?
2. What is the difference between a DAG, Operator, Sensor, and Hook?
3. What is `execution_date` in Airflow and why is it not the same as wall clock time?
4. What is `catchup=False` and when would you want catchup enabled?
5. What is the difference between Sensor `poke` and `reschedule` modes?
6. What is XCom and what are its limits?
7. Walk through an EMR job lifecycle in Airflow (4 operators).
8. Why must the terminate cluster task use `trigger_rule='all_done'`?
9. What is SparkSubmitOperator and when would you use it vs EMR operators?
10. What is the small file problem in S3/Spark and how do you fix it?
11. What is `partitionOverwriteMode=dynamic` and how does it enable idempotent writes?
12. What is S3 event-driven architecture and how does it differ from polling?
13. What is the magic S3 committer and why does it matter?
14. What three layers of monitoring does a production pipeline need?
15. What is data freshness and how do you monitor it?
16. What Spark UI metrics indicate data skew?
17. What is the Glue Schema Registry and what compatibility modes are available?
18. What is `BACKWARD` schema compatibility?
19. Walk me through a complete production batch ETL pipeline on AWS end-to-end.
20. How do you make a Spark pipeline idempotent?

---

## How to Run

```bash
docker-compose up -d
# Open http://localhost:8888
# Navigate to week14/ and run notebooks in order
# Notebooks 01-02 and 04-05 are reference guides (require Airflow/AWS to fully run)
# Notebook 03 runs locally (S3 patterns demonstrated with local filesystem)
```
