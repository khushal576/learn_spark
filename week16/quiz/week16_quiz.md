# Week 16 Quiz — Capstone & Final Review

> This is the final quiz. It covers the full course — Weeks 1 through 16. No notes.

---

## Section A: Medallion Architecture

**Q1.** A new order arrives with `user_id = null`. Which layer is it written to and what happens to it? Trace the full path from ingest to resolution.

<details><summary>Answer</summary>

1. **Bronze**: written as-is. Bronze accepts everything — no filtering. The null is preserved with `_ingested_at`, `_source`, `_run_id` columns added.
2. **Silver split**: `split_valid_dead_letter()` evaluates: `user_id IS NULL` → `_rejection_reason = "|null_user_id"`. The record goes to **dead-letter**, not Silver.
3. **Dead-letter table**: written with `mode=append` (accumulates across runs). Has `_rejection_reason` and `_rejected_at`.
4. **Silver**: record is absent. Gold is derived from Silver → also absent.
5. **Resolution**: data team inspects dead-letter. If the source is fixed upstream, the corrected record arrives in the next Bronze load → passes Silver → appears in Gold.

The null never reaches analysts. It's visible for debugging but doesn't corrupt downstream data.
</details>

---

**Q2.** Why is the dead-letter write `mode=append` while Silver write is `mode=overwrite`? What would break if you used `mode=overwrite` for dead-letter?

<details><summary>Answer</summary>

**Silver** uses overwrite (dynamic partition) because it's idempotent — re-running produces the same clean records. Overwriting is correct: you want the current clean state for that day.

**Dead-letter** uses append because:
- You want to accumulate all rejections over all runs of the same date
- If the pipeline fails and retries, a second run may reject different (or the same) records — all are worth keeping for audit
- Overwriting dead-letter on retry would lose the rejection history from the first run

If dead-letter used overwrite: re-running a failed pipeline would erase the original rejection records, making root-cause analysis harder and violating audit requirements.
</details>

---

## Section B: Delta Lake

**Q3.** You run the Silver pipeline twice for `date=2024-01-15`. Prove that the Silver row count is identical after both runs. Which specific code makes this idempotent?

<details><summary>Answer</summary>

Two mechanisms:

1. `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")` + `mode="overwrite"` + `.partitionBy("date")`: The second run completely replaces the `date=2024-01-15` partition with the same data. Other partitions are untouched.

2. `deduplicate()` uses `row_number().over(Window.partitionBy("order_id").orderBy("event_time"))` and keeps only rank=1. The same input data produces the same deduplicated output.

The Bronze source is fixed (same raw data both runs) → same valid set → same deduplication → same Silver. Row counts are identical. You can verify: `assert run1_count == run2_count`.
</details>

---

**Q4.** A colleague proposes `ALTER TABLE silver.orders SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`. What does this enable, and name two production use cases for Silver CDF.

<details><summary>Answer</summary>

CDF tracks row-level changes per commit: `insert`, `update_preimage`, `update_postimage`, `delete`. Read with:
```python
spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", N)
    .load("silver/orders")
```

**Production use cases:**
1. **Incremental Gold**: instead of re-reading all Silver every run, read only rows that changed since the last Gold run (CDF startingVersion). Much faster for large Silver tables.
2. **GDPR propagation**: after `DELETE FROM silver.orders WHERE user_id = 'X'`, read the CDF to get the set of deleted records, then propagate deletes to all downstream Gold tables — without re-running the full Gold pipeline.

Caveat: CDF adds ~10-20% write overhead. Enable only on tables where incremental reads are needed.
</details>

---

## Section C: Streaming

**Q5.** Explain what happens when the EMR Structured Streaming cluster crashes mid-batch and restarts. How does the checkpoint prevent data loss or duplication?

<details><summary>Answer</summary>

The checkpoint (on S3) stores two things:
1. **Source offsets**: which Kafka offsets have been successfully read and committed
2. **Aggregation state**: in-progress window state (partial sums, counts)

**Crash scenario:**
1. Batch N starts: reads Kafka offsets 1000-1999, begins aggregating
2. Cluster crashes before `foreachBatch` writes to Delta
3. Cluster restarts: reads checkpoint → sees batch N was not committed → replays offsets 1000-1999
4. `foreachBatch` runs again with the same data → writes same results to Delta

**No duplication**: `foreachBatch` with `mode=append` on Delta would write twice. Fix: use MERGE INTO keyed by `(window_start, region)` in `foreachBatch` — idempotent even if replayed.

**No data loss**: checkpoint ensures Spark doesn't skip ahead to offset 2000+ without processing 1000-1999.
</details>

---

**Q6.** The streaming query uses `outputMode("append")` with a watermark. A 5-minute window (09:00–09:05) receives its last event at 09:14. With a 10-minute watermark, when is this window finalized and emitted?

<details><summary>Answer</summary>

Watermark threshold = `max_event_time - watermark_duration`.

Timeline:
- At 09:14: an event arrives with `event_ts = 09:14`. `max_event_time` advances to 09:14.
- Watermark threshold = 09:14 - 10 minutes = 09:04.
- The window 09:00–09:05 ends at 09:05. Is 09:05 ≤ 09:04? No — 09:05 > 09:04, so the window is NOT yet finalized.
- At ~09:15+: another event arrives, `max_event_time` = 09:15. Threshold = 09:05. Now 09:05 ≤ 09:05 → window is finalized, aggregated, and emitted in the next trigger.

So the 09:00–09:05 window is emitted approximately 10 minutes after the window's end time. This is the latency cost of the watermark. In `append` mode, the output only appears after finalization.
</details>

---

## Section D: Orchestration

**Q7.** The Airflow DAG has `trigger_rule=TriggerRule.ALL_DONE` on `terminate_emr_cluster`. What would happen if you used the default `trigger_rule=ALL_SUCCESS` instead?

<details><summary>Answer</summary>

`ALL_SUCCESS` (default): the task only runs if ALL upstream tasks succeeded. If any EMR step fails → `terminate_emr_cluster` is **skipped** → the cluster keeps running indefinitely → billing continues (potentially hundreds of dollars/day for a 11-node cluster).

`ALL_DONE`: runs regardless of whether upstream tasks succeeded or failed. The cluster always terminates.

Real-world consequence: teams have woken up to $5,000+ surprise AWS bills from a single Airflow bug that left EMR clusters running for days. `ALL_DONE` on the termination task is non-negotiable in any production EMR pipeline.
</details>

---

**Q8.** The `wait_for_bronze` sensor uses `mode='reschedule'` and `poke_interval=900`. Why not use `poke_interval=60` with `mode='poke'`?

<details><summary>Answer</summary>

`mode='poke'` + `poke_interval=60`: the sensor holds a Celery worker slot for the entire wait duration (potentially 2 hours). If 5 DAG runs are waiting simultaneously, 5 workers are blocked — other tasks in the system queue up and SLAs are missed.

`mode='reschedule'` + `poke_interval=900`:
- Sensor releases its worker slot after each failed poke
- Reschedules itself 15 minutes later
- Only occupies a worker during the brief check (seconds), not for the full wait
- 5 waiting sensors → 5 workers freed → available for other tasks

`poke_interval=60` with reschedule would also work but creates unnecessary S3 API calls every minute. Every S3 LIST costs money; 15-minute intervals are sufficient when Bronze loads run on a 6am schedule.
</details>

---

## Section E: Testing & Production

**Q9.** Write a pytest test for `split_valid_dead_letter()` that verifies: all three rejection reasons (`null_user_id`, `non_positive_amount`, `invalid_status`) are correctly assigned to the dead-letter table.

<details><summary>Answer</summary>

```python
def test_rejection_reasons(spark):
    COLS = ["order_id", "user_id", "product", "amount", "status", "region", "event_time", "date"]
    df = spark.createDataFrame([
        ("O1", None,  "kb",  10.0,  "shipped", "US", "2024-01-15 09:00:00", "2024-01-15"),  # null user
        ("O2", "U2",  "m",   -1.0,  "shipped", "US", "2024-01-15 09:01:00", "2024-01-15"),  # bad amount
        ("O3", "U3",  "c",   10.0,  "UNKNOWN", "US", "2024-01-15 09:02:00", "2024-01-15"),  # bad status
    ], COLS)

    _, dead = split_valid_dead_letter(df)
    rows = {r["order_id"]: r["_rejection_reason"] for r in dead.collect()}

    assert "null_user_id"          in rows["O1"]
    assert "non_positive_amount"   in rows["O2"]
    assert "invalid_status"        in rows["O3"]
    assert dead.count() == 3
    # Verify no valid rows leaked into dead-letter
    assert "_rejected_at" in dead.columns
```

Key: test the _content_ of `_rejection_reason`, not just the count. A function that puts all rows in dead-letter would still pass a count-only test.
</details>

---

**Q10.** What is the purpose of `sys.exit(1)` in the production Spark script? Trace the chain of events from `sys.exit(1)` to a PagerDuty alert.

<details><summary>Answer</summary>

Chain of events:
1. Silver transform fails (e.g., S3 read error, schema mismatch)
2. Exception caught → `error()` structured log emitted → `sys.exit(1)` called
3. JVM exits with code 1 → YARN application fails → EMR records the Step as **FAILED**
4. `EmrStepSensor` polls the step status → sees FAILED → raises `AirflowException`
5. Airflow task `wait_silver_transform` → state: **FAILED**
6. `terminate_emr_cluster` runs (trigger_rule=ALL_DONE) → cluster terminated
7. `check_silver_dq` is skipped (upstream failed)
8. DAG run state: **FAILED**
9. Airflow `email_on_failure=True` → email sent to `data-eng-alerts@shopstream.com`
10. CloudWatch Alarm: EMR Step FAILED event → SNS → PagerDuty → on-call engineer paged

Without `sys.exit(1)`: the exception is caught but the process exits 0 → EMR marks step SUCCEEDED → Airflow doesn't notice → bad data silently flows to Gold → dashboard shows wrong numbers for hours.
</details>

---

## Section F: Full Course Review

**Q11.** A join between a 500 GB orders table and a 50 MB products table is running in 40 minutes. How would you optimize it and what would you expect the runtime to drop to?

<details><summary>Answer</summary>

This is a classic broadcast join opportunity:
1. Products table (50 MB) fits easily in executor memory
2. Add broadcast hint: `orders.join(broadcast(products), "product_id")`
3. Or raise threshold: `spark.sql.autoBroadcastJoinThreshold=100m`

What changes: the 50 MB products table is sent to every executor once. The 500 GB orders table doesn't shuffle at all — each executor processes its local partition and looks up product data locally.

Before: sort-merge join → shuffle 500 GB of orders across the network → 40 min (mostly shuffle I/O)
After: broadcast join → zero shuffle for orders → expected runtime: ~5-10 min (compute-bound)

Also check: AQE would auto-detect this if the products table was small enough and `spark.sql.adaptive.enabled=true`. If it didn't auto-broadcast, the stats estimate may be wrong — force with the hint.
</details>

---

**Q12.** Design a freshness monitoring system for ShopStream. What metrics would you track, how would you alert, and what is the recovery procedure for a freshness violation?

<details><summary>Answer</summary>

**Metrics to track:**
- `gold_last_updated_at`: MAX(`_updated_at`) from Gold tables
- `silver_last_updated_at`: MAX(`_updated_at`) or last partition written
- `bronze_row_count`: rows ingested today vs yesterday (% change)
- `pipeline_duration_s`: time from DAG start to Gold write

**Alert thresholds:**
- Gold not updated within 2h of scheduled completion → P1 (PagerDuty)
- Gold not updated within 4h → P0 (wake the on-call engineer)
- Silver row count drops >50% vs 7-day avg → P2 (Slack)
- Pipeline duration > 3× baseline → P2 (Slack, investigate skew)

**Implementation:** Spark job writes DQ JSON metrics to S3 → CloudWatch metric filter parses the JSON → CloudWatch alarm triggers SNS → PagerDuty/Slack.

**Recovery procedure for freshness violation:**
1. Check Airflow: which task failed?
2. If EMR step failed: check EMR step logs (YARN container logs)
3. If Bronze is missing: upstream issue (DMS stopped, RDS down) — page upstream team
4. If Silver/Gold failed: fix code or config, re-trigger DAG from the failed task
5. If QuickSight stale: all layers fine, force SPICE refresh manually
6. Notify stakeholders: estimated resolution time + root cause
7. Post-mortem: add monitoring to detect the root cause earlier next time
</details>
