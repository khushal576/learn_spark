# Week 13: Phase 7 — Cloud Platforms Deep Dive & Lakehouse Architecture

> **Phase 7 → Week 13**
>
> Prerequisites: Week 12 (cluster config, advanced joins, Pandas UDFs, cloud overview)

---

## What You'll Learn This Week

Week 13 goes deep on the cloud platform topics introduced in Week 12, adds Delta Lake advanced patterns, and covers Lakehouse architecture and governance — the knowledge that separates a junior data engineer from a senior one.

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [Delta Lake Advanced](01_delta_lake_advanced.ipynb) | Multi-action MERGE, Change Data Feed, schema evolution, RESTORE, OPTIMIZE ZORDER |
| 02 | [Databricks Deep Dive](02_databricks_deep_dive.ipynb) | Unity Catalog, DLT pipelines, Workflows orchestration, Auto Loader, cost optimization |
| 03 | [EMR Deep Dive](03_emr_deep_dive.ipynb) | Instance groups, Bootstrap actions, Spot strategies, EMR Serverless, Step Functions |
| 04 | [Glue Advanced](04_glue_advanced.ipynb) | Data Catalog, Crawlers, Bookmarks, DynamicFrame API, Glue vs EMR decision matrix |
| 05 | [Lakehouse Architecture](05_lakehouse_architecture.ipynb) | Medallion deep dive, Delta vs Iceberg vs Hudi, governance, data mesh |

---

## Key Rules to Remember

### Delta Lake Advanced
- CDF (`delta.enableChangeDataFeed=true`): records insert/update_preimage/update_postimage/delete per row — read with `readChangeFeed` option
- Multi-action MERGE: one statement can DELETE matched + UPDATE matched + INSERT new
- `mergeSchema=true`: add new columns silently; `overwriteSchema=true`: replace schema (destructive)
- OPTIMIZE + ZORDER: compacts small files AND co-locates rows by column value for data skipping
- VACUUM removes old Parquet files — after VACUUM, time travel to those versions is impossible
- Default retention: 7 days. Set `delta.deletedFileRetentionDuration` to control

### Databricks
- All-Purpose clusters: interactive dev (~$0.40/DBU); Job Clusters: production (~$0.15/DBU)
- Unity Catalog: three-level namespace (`catalog.schema.table`), row/column security, lineage
- DLT expectations: `expect` (warn), `expect_or_drop` (filter), `expect_or_fail` (halt pipeline)
- Auto Loader: event-driven incremental S3 ingest, handles schema inference + rescue column
- Spot workers: 60-80% cheaper; never Spot the driver node (interruption kills the job)

### EMR
- Instance groups: MASTER (On-Demand always), CORE (On-Demand if using HDFS), TASK (always Spot)
- Bootstrap actions: shell scripts on every node before Spark starts — use `set -ex`, keep fast
- `maximizeResourceAllocation=true`: EMR auto-sizes executors for the instance type
- EMR Serverless: pay per vCPU-second, no idle cost, ~30-60s cold start, no streaming support
- S3 committer (`fs.s3a.committer.name=magic`): atomic writes without COPY+DELETE overhead

### Glue
- Glue Data Catalog: one per account-per-region, shared by Athena/EMR/Glue/Redshift Spectrum
- Crawlers: auto-discover schema + partitions; `CRAWL_NEW_FOLDERS_ONLY` for incremental
- Bookmarks: Glue tracks processed S3 files, only reads new ones on next run
- DynamicFrame: schema-flexible, handles mixed types with `resolveChoice()`; convert with `.toDF()` for complex transforms
- Glue wins: small team, sporadic jobs, Athena-first; EMR wins: streaming, large jobs, custom tuning

### Lakehouse Architecture
- Medallion: Bronze (raw + metadata), Silver (validated + deduped + typed), Gold (aggregated, business-ready)
- Each layer should be idempotent: re-running produces the same result
- Delta Lake: best Spark/Databricks fit; Iceberg: best multi-engine; Hudi: best high-velocity upserts
- Iceberg hidden partitioning: declare transforms, not partition columns — queries never need to know partition scheme
- Data Mesh: domains own data products; platform provides infra; governance federated via policy

---

## Interview Questions Covered

1. What is Delta Lake's Change Data Feed and when would you use it?
2. What is the difference between `mergeSchema` and `overwriteSchema`?
3. What does OPTIMIZE ZORDER BY do at the file level?
4. What is VACUUM and what is the risk of running it with 0 hours retention?
5. What is Databricks Unity Catalog and what governance features does it provide?
6. What is Delta Live Tables (DLT) and how does it differ from writing Spark jobs manually?
7. What is the difference between All-Purpose and Job Clusters in Databricks?
8. What is Auto Loader and how does it differ from a regular file source?
9. What is the role of each EMR instance group (Master, Core, Task)?
10. What is a Glue Bootstrap Action? What are best practices?
11. What is EMR Serverless and when would you choose it over regular EMR?
12. What is the Glue Data Catalog and why use it instead of a local Hive metastore?
13. What are Glue Crawlers and what schema change policies matter?
14. What is a Glue Job Bookmark and how does it enable incremental processing?
15. What is a Lakehouse and how does it differ from a Data Lake?
16. Compare Delta Lake, Iceberg, and Hudi — when would you choose each?
17. What is Iceberg hidden partitioning and why is it better than explicit partitioning?
18. What are the four principles of Data Mesh?
19. How would you handle a GDPR right-to-erasure request for data in a Delta Lake?
20. What is AWS Lake Formation and how does it improve on IAM bucket policies?

---

## How to Run

```bash
docker-compose up -d
# Open http://localhost:8888
# Navigate to week13/ and run notebooks in order
# Note: notebooks 02-04 are reference guides (cloud patterns, not locally runnable)
# Notebooks 01 and 05 run fully locally with Delta Lake
```
