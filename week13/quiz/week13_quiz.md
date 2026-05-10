# Week 13 Quiz — Cloud Platforms Deep Dive & Lakehouse Architecture

> Test yourself after completing all 5 notebooks. No notes.

---

## Section A: Delta Lake Advanced

**Q1.** What is Delta Lake's Change Data Feed (CDF)? How do you enable it and how do you read it? What are the four possible values of `_change_type`?

<details><summary>Answer</summary>

CDF records every row-level change as a new version in the Delta log. Enable with `delta.enableChangeDataFeed=true` as a table property. Read with:
```python
spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", N)
    .load(path)
```
`_change_type` values: `insert`, `update_preimage` (old row before update), `update_postimage` (new row after update), `delete`.
</details>

---

**Q2.** Write a MERGE INTO statement that: updates `email` and `tier` when the customer exists, deletes the row when `tier='deleted'`, and inserts new rows. What is the order of WHEN clauses?

<details><summary>Answer</summary>

```python
dt.alias("t").merge(updates.alias("s"), "t.id = s.id") \
    .whenMatchedDelete(condition="s.tier = 'deleted'") \
    .whenMatchedUpdate(set={"email": "s.email", "tier": "s.tier"}) \
    .whenNotMatchedInsert(values={"id": "s.id", "email": "s.email", "tier": "s.tier"}) \
    .execute()
```
Order matters: DELETE check must come before UPDATE (once matched, only one action fires).
</details>

---

**Q3.** What is the difference between `mergeSchema=true` and `overwriteSchema=true`? Give a scenario where each would break a downstream consumer.

<details><summary>Answer</summary>

`mergeSchema=true` (append): adds new columns from the incoming DataFrame to the table schema. Old rows get `NULL` for the new columns. Downstream breaks if consumer has `NOT NULL` constraints or explicit SELECT * with fixed column count.

`overwriteSchema=true` (overwrite): replaces the entire table schema with the new one. Downstream breaks if consumer references columns that no longer exist, or if column types change.
</details>

---

**Q4.** What does `OPTIMIZE ZORDER BY (customer_id)` do physically? When does ZORDER stop helping?

<details><summary>Answer</summary>

OPTIMIZE compacts many small Parquet files into fewer large files (target ~1 GB each). ZORDER BY re-sorts the data within each file so rows with the same `customer_id` are co-located. This enables Delta's data skipping: when querying `WHERE customer_id = X`, Delta reads the min/max stats per file and skips files that can't contain X.

ZORDER stops helping when: (1) cardinality is too low (3 unique values → all files touched anyway), (2) the column is never used in filters, (3) you ZORDER by too many columns (co-location degrades with dimensionality), (4) after VACUUM removes old files, historical versions lose their layout.
</details>

---

**Q5.** You accidentally overwrite a production Delta table. What steps do you take to restore it? What is the minimum retention needed for this to work?

<details><summary>Answer</summary>

1. Check history: `DeltaTable.forPath(spark, path).history().show()`
2. Find the last good version (e.g., version 5)
3. Restore: `DeltaTable.forPath(spark, path).restoreToVersion(5)`
   Or SQL: `RESTORE TABLE my_table TO VERSION AS OF 5`

For this to work, the old Parquet files must still exist (not VACUUM'd). Default retention is 7 days (`delta.deletedFileRetentionDuration`). If VACUUM ran with retention 0 or a very short window, the files are gone and restoration is impossible.
</details>

---

## Section B: Databricks

**Q6.** What is the three-level namespace in Unity Catalog? Give an example. How does it differ from the old per-workspace Hive metastore?

<details><summary>Answer</summary>

`catalog.schema.table` — e.g., `prod_catalog.silver.orders`.

Old Hive metastore: each workspace had its own isolated `database.table` namespace. Tables in workspace A were not visible in workspace B. 

Unity Catalog: one centralized catalog spans all workspaces (and can span clouds). Fine-grained access control (GRANT/REVOKE at table/column/row level), lineage tracking, and audit logs are all centralized.
</details>

---

**Q7.** What are the three DLT expectation decorators? What happens to data and to the pipeline for each?

<details><summary>Answer</summary>

| Decorator | Data behavior | Pipeline behavior |
|-----------|--------------|-------------------|
| `@dlt.expect("name", "condition")` | Invalid rows pass through | Pipeline continues, metric tracked |
| `@dlt.expect_or_drop("name", "condition")` | Invalid rows dropped | Pipeline continues |
| `@dlt.expect_or_fail("name", "condition")` | Any invalid row | Pipeline halts with error |
</details>

---

**Q8.** What is Auto Loader's rescue column (`_rescued_data`)? When does it appear and how should a production pipeline handle it?

<details><summary>Answer</summary>

When `cloudFiles.schemaEvolutionMode=rescue` (or `addNewColumns`), any field that doesn't match the known schema is captured in `_rescued_data` as a JSON string. It appears when: source produces new fields not in the declared schema.

Production handling: (1) Alert when `_rescued_data` is non-null (schema change detected), (2) Parse the rescued column to extract new fields, (3) Update the schema declaration after review, (4) Never silently discard rescued data.
</details>

---

**Q9.** You have a Databricks All-Purpose cluster running 24/7 for one team. It costs $3,000/month. What three changes would you make to cut cost without breaking their workflow?

<details><summary>Answer</summary>

1. **Auto-terminate**: set idle timeout to 30-60 minutes. If they work business hours (8 hrs/day, 22 days/month): 8 × 22 / 24 / 30 ≈ 27% utilization → save ~73% of compute hours.
2. **Spot workers**: keep On-Demand driver, switch worker nodes to Spot. 70% worker cost savings.
3. **Right-size**: check Spark UI for executor CPU/memory utilization. If consistently < 50%, halve the worker count.
Combined savings potential: 80-90% cost reduction.
</details>

---

## Section C: EMR & Glue

**Q10.** What is the difference between EMR's CORE and TASK instance groups? Which can safely run on Spot and why?

<details><summary>Answer</summary>

**CORE**: has both YARN NodeManager (compute) AND HDFS DataNode (storage). Losing a CORE node loses HDFS data blocks. Safe for Spot only if reading from S3 (no HDFS), and with HDFS replication factor ≥ 3 if HDFS is used.

**TASK**: has only YARN NodeManager (pure compute). No HDFS data. In-flight tasks are redistributed when node is reclaimed. Always safe to use Spot for TASK nodes — this is the primary cost-saving lever in EMR.
</details>

---

**Q11.** What is EMR Serverless? When would you choose it over a regular EMR cluster? What are its limitations?

<details><summary>Answer</summary>

EMR Serverless: fully managed Spark runtime. Submit a job → AWS provisions capacity → job runs → AWS deprovisions. Pay per vCPU-second + GB-second of memory. No idle cost.

Choose Serverless when: sporadic jobs (< 5/day), no Spark/cluster expertise, want zero infra management, OK with ~30-60 second cold start.

Limitations: no Structured Streaming, cold start latency (mitigated with pre-initialized capacity), less tuning control, no Spark UI access during run (logs only), higher per-unit cost than optimally-tuned Spot clusters.
</details>

---

**Q12.** What is a Glue Crawler's `SchemaChangePolicy`? What are the two sub-policies and what are the safe vs aggressive options?

<details><summary>Answer</summary>

`UpdateBehavior`: what to do when new columns are detected.
- `UPDATE_IN_DATABASE`: auto-update the Glue Catalog table schema (safe for additive changes, risky if columns are removed or types change)
- `LOG`: log changes but don't update (safe, requires manual review)

`DeleteBehavior`: what to do when an S3 path/partition disappears.
- `DELETE_FROM_DATABASE`: remove the table (aggressive — accidental data delete could remove catalog entry)
- `DEPRECATE_IN_DATABASE`: mark as deprecated (safer, recoverable)
- `LOG`: log only (safest)
</details>

---

**Q13.** Glue Job Bookmarks track processed S3 files. What are two scenarios where bookmarks will NOT work as expected?

<details><summary>Answer</summary>

1. **Modified files**: bookmarks track S3 ETag/size/timestamp at write time. If a file is modified after being processed, bookmarks won't reprocess it (they track file identity, not content changes). Use a custom watermark on `last_modified` timestamp for CDC.

2. **Job renamed**: the bookmark is stored under the job name. Renaming the Glue job starts fresh — re-reads all files. Always use stable job names in production.

Other limitations: only works with S3 sources (not JDBC), doesn't work with `DynamicFrame.fromDF()` (must use `from_catalog` or `from_options`).
</details>

---

## Section D: Lakehouse & Governance

**Q14.** What are the three layers of the Medallion Architecture and what is the key rule for each?

<details><summary>Answer</summary>

**Bronze**: raw data, as-is from the source. Add ingestion metadata (`_ingested_at`, `_source`, `_run_id`). Never filter or transform. Keep everything — including bad records. Append-only.

**Silver**: validated, deduplicated, typed. Apply business rules (null checks, range validation, referential integrity). Cast types. Deduplication. Make each run idempotent with MERGE. Quarantine bad records to a dead-letter table.

**Gold**: aggregated, business-ready. Purpose-built for specific use cases (reporting, ML features, dashboards). Rebuild from Silver (idempotent overwrite or MERGE). These are the tables analysts and BI tools query.
</details>

---

**Q15.** Compare Delta Lake and Apache Iceberg. For a team using Spark + Flink + Trino, which would you recommend?

<details><summary>Answer</summary>

| | Delta Lake | Apache Iceberg |
|--|--|--|
| Spark | Excellent | Good |
| Flink | Limited | Excellent (native) |
| Trino | Good | Excellent (native) |
| Multi-engine | Databricks-centric | Truly open |
| Hidden partition | No | Yes |
| Auto-optimize | Auto Loader, OPTIMIZE | Table maintenance tasks |

**Recommendation: Apache Iceberg** for Spark + Flink + Trino. All three engines have first-class Iceberg support. Hidden partitioning means the team can change partition schemes without rewriting queries. Avoids Databricks vendor lock-in.

Choose Delta only if the team is committed to Databricks and wants tighter ecosystem integration.
</details>

---

**Q16.** What is Iceberg's "hidden partitioning" feature? Give an example showing why it's better than explicit partitioning.

<details><summary>Answer</summary>

In Delta/Hive, partitioning is explicit: you physically partition by a column (e.g., `date`), and every query must filter by that exact column name: `WHERE date = '2024-01-15'`.

In Iceberg, you declare partition transforms on existing columns:
```sql
CREATE TABLE orders (..., event_ts TIMESTAMP)
PARTITIONED BY (day(event_ts));
```
Query: `WHERE event_ts BETWEEN '2024-01-15' AND '2024-01-16'` — Iceberg automatically prunes partitions without you mentioning `day`.

When you change from daily to hourly partitioning:
- Delta: rewrite all data + update all queries
- Iceberg: `ALTER TABLE REPLACE PARTITION FIELD day(event_ts) WITH hour(event_ts)` — old data keeps old layout, new data uses new layout, queries unchanged.
</details>

---

**Q17.** A user exercises GDPR right-to-erasure. Their data is in a Bronze Delta table (append-only by design) and a Silver table. How do you handle deletion from each?

<details><summary>Answer</summary>

**Silver (easier)**: Delta supports DELETE: `DELETE FROM silver.orders WHERE customer_id = 'C001'`. Then VACUUM to physically remove the old Parquet files containing that user's data (after retention period).

**Bronze (harder)**: Bronze is append-only but Delta still supports DELETE. Run `DELETE FROM bronze.orders WHERE customer_id = 'C001'`. Complications:
1. VACUUM must run after retention period to actually remove physical files
2. Until VACUUM, the data still exists in old Parquet files (accessible via time travel)
3. Must disable time travel for compliance: reduce retention duration before VACUUM
4. If Bronze is raw JSON/CSV (not Delta), you must rewrite the entire file without that user's rows

Production approach: maintain a "tombstone" table of deleted customer IDs, apply at read time, and run a quarterly physical deletion with VACUUM.
</details>

---

**Q18.** What are the four principles of Data Mesh? Which is the hardest to implement and why?

<details><summary>Answer</summary>

1. **Domain-oriented ownership**: domain teams own their data end-to-end
2. **Data as a product**: each domain publishes data products with documented SLAs
3. **Self-serve data platform**: platform team provides infrastructure, not data
4. **Federated computational governance**: global policies enforced automatically

The hardest is **Data as a product** (principle 2). It requires domain engineering teams to take on data quality, freshness guarantees, documentation, and consumer support — responsibilities they typically aren't staffed or incentivized for. Without this, data mesh degrades into "everyone does their own thing" with no discoverability or trust.
</details>
