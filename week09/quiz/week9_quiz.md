# Week 9 Quiz — PySpark ETL Engineering Part 2

> Answer without looking at the notebooks. Check answers afterward.

---

## Section 1: Delta Lake (12 points)

**Q1.** What is the `_delta_log` directory? What is stored in it and how does Spark use it to reconstruct the current state of a Delta table?

**Q2.** You wrote three versions to a Delta table: version 0 (3 rows), version 1 (appended 2 rows), version 2 (overwrite). Write code to read the table exactly as it was at version 1.

**Q3.** What is the difference between `whenMatchedUpdateAll()` and `whenMatchedUpdate(condition=..., set={...})` in a MERGE operation? When would you use the conditional form?

**Q4.** You run `VACUUM delta./path/to/table` on a table with 14 days of history. What happens? Can you still time-travel to version 0 from 14 days ago?

**Q5.** A new upstream column `region` appears in some files but not others. You try to append to your Delta table and get an `AnalysisException`. What option do you add to the write call and what is the effect on existing rows?

**Q6.** What is Delta Change Data Feed? What does `_change_type = 'update_preimage'` mean vs `'update_postimage'`? Give a use case for CDF.

---

## Section 2: ETL Design Patterns (10 points)

**Q7.** Your `customers` table has `name`, `email`, `city`. A customer moves from Mumbai to Pune. Describe how SCD Type 1 handles this vs SCD Type 2. What columns does SCD Type 2 require that SCD Type 1 does not?

**Q8.** Write the MERGE statement (in pseudocode or PySpark) that implements SCD Type 2 for a `customers` table matched on `customer_id`. The merge should close old rows on change and insert new rows. What condition determines whether a row has "changed"?

**Q9.** In the Medallion Architecture, why is the Bronze layer append-only and schema-on-read? What would you lose if you applied transformations at the Bronze stage?

**Q10.** What is idempotent ETL? You process a date partition `order_date='2024-01-15'`. The job fails halfway and you re-run it. What write option ensures the re-run produces identical results to a single successful run?

**Q11.** You have 50M rows to deduplicate, keeping the latest record per `order_id` by `updated_at`. Why is `dropDuplicates(["order_id"])` not sufficient here? Write the correct approach using Window functions.

---

## Section 3: Error Handling & Monitoring (8 points)

**Q12.** You write a UDF that parses a date string. It raises `ValueError` for malformed input. What happens to the Spark task if you don't catch the exception? How do you fix it?

**Q13.** You have an Accumulator `error_count`. You increment it inside a `map()` transformation. When is it safe to read `error_count.value` from the driver? Why?

**Q14.** Name three metrics your pipeline should track per stage, and two conditions that should trigger an alert.

**Q15.** Your pipeline connects to an external database that occasionally times out. Write a `run_with_retry` function with exponential backoff (max 3 retries, base delay 2 seconds). What is the wait time before each retry?

---

## Section 4: Testing PySpark (6 points)

**Q16.** Why should transformation logic be separated from SparkSession creation in production code? How does this separation make unit testing easier?

**Q17.** You have a `normalize_status` function that lowercases the `status` column. Write a complete pytest test that: creates a small DataFrame, calls the function, and asserts the output using `assert_df_equal`. Include the edge case of an empty DataFrame.

**Q18.** Why use `@pytest.fixture(scope='session')` for the SparkSession in tests? What happens if you use `scope='function'` instead?

---

## Section 5: Production Deployment (4 points)

**Q19.** What is the difference between `--deploy-mode client` and `--deploy-mode cluster` in `spark-submit`? Which do you use in production on YARN and why?

**Q20.** Your Spark job depends on a custom Python package `myutils`. The package is not installed on cluster nodes. What are two ways to make it available, and which is preferred for reproducibility?

---

## Answers

### Q1
The `_delta_log` directory contains a JSON file for every transaction (e.g., `00000000000000000000.json`). Each entry records which Parquet files were **added** and which were **removed** in that transaction. Spark reconstructs the current table by reading the latest checkpoint (a consolidated `.parquet` file created every 10 transactions) and replaying all JSON entries after it. This log is what enables ACID (a transaction is one atomic JSON entry), time travel (replay only up to version N), and concurrent write detection (optimistic concurrency via log conflicts).

---

### Q2
```python
spark.read.format("delta").option("versionAsOf", 1).load("/path/to/table")
```
Version 1 had 5 rows (3 original + 2 appended). Version 2 was an overwrite, so `versionAsOf=1` reads the pre-overwrite state.

---

### Q3
- `whenMatchedUpdateAll()` — updates every column in the target to the source value when the join key matches, unconditionally.
- `whenMatchedUpdate(condition="s.status != t.status", set={"status": "s.status"})` — only updates if the specified condition is true, and only updates the listed columns.

Use the conditional form when you want to avoid no-op writes (don't rewrite a row if nothing changed) or when you only want to update specific columns rather than all of them.

---

### Q4
`VACUUM` deletes Parquet files that are no longer referenced by any Delta log version **within the retention period** (default 7 days). After VACUUM, the old files are gone from disk. You **cannot** time-travel to a version whose Parquet files have been deleted — you will get an `AnalysisException`. So if version 0 is 14 days old and retention is 7 days, time travel to version 0 fails after VACUUM.

---

### Q5
```python
df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
```
`mergeSchema=True` tells Delta to add the new column to the table schema. Existing rows that were written before the column existed will have **null** for that column.

---

### Q6
Change Data Feed (CDF) records row-level changes as Delta operations:
- `update_preimage`: the row's values **before** the update
- `update_postimage`: the row's values **after** the update
- `insert`: a newly inserted row
- `delete`: a row that was deleted

Read with: `spark.read.format("delta").option("readChangeFeed","true").option("startingVersion",1).load(path)`

**Use case**: downstream incremental pipelines that only need to process what changed (instead of re-reading the whole table), CDC to replicate Delta changes into another system.

---

### Q7
**SCD Type 1** — Overwrite in place. The city is updated from Mumbai to Pune. History is lost — you cannot tell the customer ever lived in Mumbai. No extra columns needed.

**SCD Type 2** — The old Mumbai row is "closed" by setting `end_date = today` and `is_current = False`. A new Pune row is inserted with `start_date = today`, `end_date = NULL`, `is_current = True`. Full history is preserved.

Extra columns SCD2 requires: `start_date`, `end_date`, `is_current` (and typically a surrogate `customer_sk` primary key).

---

### Q8
```python
change_condition = "s.name != t.name OR s.email != t.email OR s.city != t.city"

# Step 1: Close old rows that changed
target.alias("t") \
  .merge(incoming.alias("s"), "s.customer_id = t.customer_id AND t.is_current = true") \
  .whenMatchedUpdate(
      condition=change_condition,
      set={"end_date": F.current_date(), "is_current": F.lit(False)}
  ).execute()

# Step 2: Insert new rows for changed + new customers
new_rows = incoming.join(
    spark.read.format("delta").load(path).filter("is_current = false"),
    "customer_id", "inner"
).union(incoming.join(spark.read.format("delta").load(path), "customer_id", "left_anti"))

new_rows.withColumn("start_date", F.current_date()) \
        .withColumn("end_date", F.lit(None)) \
        .withColumn("is_current", F.lit(True)) \
        .write.format("delta").mode("append").save(path)
```
The change condition checks whether any tracked column differs between source and target.

---

### Q9
Bronze is append-only because it is the **raw landing zone** — the source of truth for what was received. If you transform at Bronze, you lose the ability to reprocess with new logic, you cannot audit what the source actually sent, and errors in your transform logic corrupt the only copy of the data. Schema-on-read means Bronze accepts whatever schema arrives without enforcing structure — schema enforcement and cleaning happen in the Silver step.

---

### Q10
An idempotent ETL pipeline produces the same result whether run once or multiple times. For a date partition:
```python
batch.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "order_date = '2024-01-15'") \
    .save(path)
```
`replaceWhere` atomically replaces only the matching partition, so a re-run produces identical data rather than duplicating it.

---

### Q11
`dropDuplicates(["order_id"])` is non-deterministic — it keeps an arbitrary row when there are duplicates on `order_id`, not necessarily the one with the latest `updated_at`.

Correct approach:
```python
from pyspark.sql import Window

w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
dedup = df.withColumn("_rn", F.row_number().over(w)) \
          .filter(F.col("_rn") == 1) \
          .drop("_rn")
```
`row_number()` assigns rank 1 to the most recent row per `order_id`, deterministically.

---

### Q12
If a UDF raises an unhandled exception, the Spark **task fails**. Spark retries the task (default 4 times). If it keeps failing, the **job fails** entirely.

Fix: wrap the UDF body in try/except and return `None` on error:
```python
@udf(DoubleType())
def safe_parse_amount(s):
    try:
        return float(s)
    except Exception:
        return None
```
This converts bad input to null rather than crashing the pipeline.

---

### Q13
Read `error_count.value` **after calling an action** (`.count()`, `.collect()`, `.write`) that triggers the transformation containing the accumulator increment. Accumulators are only guaranteed to be updated after the action completes — reading them inside a lazy transformation gives undefined results because the transformation has not run yet.

---

### Q14
Metrics per stage:
- `input_rows` — how many rows entered the stage
- `output_rows` — how many rows passed through
- `rejected_rows` — how many were sent to dead letter
- `duration_s` — wall-clock time for the stage

Alert conditions (any two of):
- Rejection rate > threshold (e.g., > 5% of input rows rejected)
- Duration > SLA (e.g., stage took > 2× average)
- Output row count dropped by more than N% vs yesterday's run
- Zero output rows when input was non-zero

---

### Q15
```python
import time

def run_with_retry(fn, max_retries=3, backoff_s=2):
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == max_retries:
                raise
            wait = backoff_s * (2 ** (attempt - 1))
            time.sleep(wait)
```
Wait times: attempt 1 fails → wait 2s; attempt 2 fails → wait 4s; attempt 3 fails → raise.

---

### Q16
If transformation logic depends on `spark.createDataFrame()` or reads from disk, your unit test must spin up a SparkSession and manage test data files. By separating logic into pure functions that accept DataFrames and return DataFrames, you can create a small test DataFrame with known values, call the function, and assert the output — no file I/O, no SparkSession per test. This also makes functions reusable across pipelines.

---

### Q17
```python
# transforms.py
def normalize_status(df):
    return df.withColumn("status", F.lower(F.col("status")))

# test_transforms.py
def test_normalize_status(spark):
    input_df = spark.createDataFrame([
        ("O001", "PENDING"), ("O002", "Delivered"),
    ], ["order_id", "status"])

    result = normalize_status(input_df)
    expected = spark.createDataFrame([
        ("O001", "pending"), ("O002", "delivered"),
    ], ["order_id", "status"])
    assert_df_equal(result, expected, sort_cols=["order_id"])

def test_normalize_status_empty(spark):
    schema = StructType([StructField("order_id", StringType()), StructField("status", StringType())])
    empty = spark.createDataFrame([], schema)
    result = normalize_status(empty)
    assert result.count() == 0
```

---

### Q18
`scope='session'` creates **one** SparkSession shared across all tests in the pytest run. Creating a SparkSession takes ~5–10 seconds (JVM startup). With `scope='function'`, a new SparkSession is created and torn down for every single test — a 20-test suite would take 2+ minutes just on startup overhead. Use `scope='session'` to pay the cost once.

---

### Q19
- **`--deploy-mode client`** — The driver runs on the machine where you called `spark-submit` (your laptop or edge node). The executors run on the cluster. Good for interactive development and debugging (logs printed to your terminal).
- **`--deploy-mode cluster`** — The driver runs on one of the cluster nodes. Your laptop is only used to submit the job and can disconnect. Preferred in production because: the driver is close to the executors (low latency), your laptop going offline doesn't kill the job, and logs are captured on the cluster.

---

### Q20
**Option 1: `--py-files`**
```bash
spark-submit --py-files myutils.zip my_job.py
```
Zip the package and distribute it to all worker nodes via Spark's file distribution mechanism.

**Option 2: Pre-install on cluster nodes**
Use a bootstrap script (EMR bootstrap action, Docker image, Conda environment) to install the package on all nodes before the job starts.

**Preferred**: `--py-files` (or a virtualenv archive) because it is version-pinned per job — each job brings its own dependencies, preventing version conflicts between jobs on the same cluster. Pre-install is fragile because all jobs share the same environment.
