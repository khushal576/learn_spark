# Week 8 Quiz — PySpark ETL Engineering Part 1

> Answer without looking at the notebooks. Check answers afterward.

---

## Section 1: Reading Data Sources (10 points)

**Q1.** What is the difference between `PERMISSIVE`, `DROPMALFORMED`, and `FAILFAST` read modes in Spark CSV/JSON? When would you use each in production?

**Q2.** You define `StructField("amount", DoubleType(), nullable=True)` but the CSV has `"abc"` in the amount column. What happens in PERMISSIVE mode?

**Q3.** You have files in `/data/events/year=2024/month=01/day=15/`. Write the Spark code to read all files and filter only `year=2024, month=01` efficiently. How does Spark avoid reading other directories?

**Q4.** What is `mergeSchema=True` and when do you need it?

**Q5.** You want to read a PostgreSQL table with 10M rows using JDBC. What options must you set to avoid reading everything in a single task?

---

## Section 2: Writing & File Formats (8 points)

**Q6.** Your pipeline writes to `/output/orders` daily. Today's run should NOT affect data from previous days. Which write mode do you use and why?

**Q7.** A DataFrame has 200 shuffle partitions. You write it to Parquet. How many files are created? Write code to reduce this to 4 files without a full shuffle.

**Q8.** When is `partitionBy("customer_id")` a bad idea? What should you use for join optimization on customer_id instead?

**Q9.** Compare Parquet vs CSV for production analytics: give 4 specific advantages of Parquet.

---

## Section 3: Schema Management (8 points)

**Q10.** Write a `StructType` schema for this table: `user_id` (string, not null), `age` (int, nullable), `signup_date` (date), `preferences` (map of string to string).

**Q11.** Your pipeline reads JSON where some records have 5 fields and others have 7 (different vendor versions). How do you handle this without failing?

**Q12.** What is the difference between `cast()` failing silently vs `FAILFAST` mode? Give a scenario where each is appropriate.

**Q13.** You have a nested JSON with `address.city` and `address.country`. Write code to flatten these into `address_city` and `address_country` top-level columns.

---

## Section 4: Data Quality (8 points)

**Q14.** Write a one-line PySpark expression to count null values for every column in a DataFrame simultaneously (not N separate jobs).

**Q15.** Describe the dead letter pattern. What 3 pieces of metadata should every dead letter record contain?

**Q16.** `df.dropDuplicates(["order_id"])` vs keeping the latest record via `row_number()` — what is the difference and when would incorrect dedup cause a production incident?

**Q17.** You check null% for `customer_id` and find 15% nulls. What are the three possible responses in a production pipeline?

---

## Section 5: Incremental Patterns (6 points)

**Q18.** Your watermark-based ETL has these steps: (1) read records where `modified_at > watermark`, (2) write to output, (3) update watermark. The job crashes between step 2 and 3. What happens on the next run? Is this a problem?

**Q19.** What is the difference between a watermark-based incremental load and a partition-based incremental load? Give a use case for each.

**Q20.** Why is `write.mode("append")` insufficient for an upsert? What does it do that you DON'T want?

---

## Answers

<details>
<summary>Click to reveal answers</summary>

**A1.**
- `PERMISSIVE` (default): bad fields become null; entire row placed in `_corrupt_record` column. **Use in production** — captures bad rows for review without crashing the pipeline.
- `DROPMALFORMED`: silently drops bad rows — row count decreases without any notification. **Use only if bad rows are truly unrecoverable** and you have row count monitoring to detect drops.
- `FAILFAST`: throws exception on the first malformed row. **Use in development** to catch schema issues immediately, or in pipelines where ANY bad row is unacceptable (financial compliance).

**A2.** In `PERMISSIVE` mode, `"abc"` cast to `DoubleType` becomes `null`. The row is included in the result with `amount = null`. The original full row text is captured in `_corrupt_record` IF you added that column to the schema. No exception is thrown.

**A3.**
```python
df = spark.read.parquet("/data/events")
# Partition pruning: Spark reads only matching directories
df.filter((F.col("year") == "2024") & (F.col("month") == "01"))
# In explain(): PartitionFilters: [year=2024, month=01]
```
Spark uses partition discovery — it knows which directories map to which values. When you filter on those columns, only matching directories are opened (partition pruning). No data from other partitions is read.

**A4.** `mergeSchema=True` unions all schemas found across files in the read path. Needed when: files were written at different times with different column sets (e.g., a new column was added in later batches). Without it, Spark uses the schema of the first file it encounters — newer columns are silently dropped or cause errors. Required for schema evolution scenarios.

**A5.**
```python
spark.read.format("jdbc") \
  .option("partitionColumn", "id")    # must be numeric
  .option("lowerBound", "1")
  .option("upperBound", "10000000")
  .option("numPartitions", "20")      # 20 parallel DB connections
  .option("url", "jdbc:postgresql://...")
  .option("dbtable", "orders")
  .load()
```
Without these options, all 10M rows are read in a single task on the driver — slow, risks OOM.

**A6.** `mode("append")` — because `overwrite` would delete all previous days' data. Append adds today's data alongside existing data. However, append does NOT prevent duplicates if the job is re-run. For idempotent appends, use partition overwrite: write to a specific date partition with `overwrite` mode (only that partition is replaced).

**A7.**
```python
# 200 files by default (one per partition)
df.write.parquet("/output")  # 200 files

# Reduce to 4 files using coalesce (no shuffle)
df.coalesce(4).write.parquet("/output")  # 4 files, no full shuffle
```

**A8.** `partitionBy("customer_id")` is bad when `customer_id` has high cardinality (e.g., 1M unique values) — it creates 1M directories, each with tiny files. This causes HDFS NameNode metadata overflow and Spark driver memory issues when listing files. For join optimization on `customer_id`, use `bucketBy(N, "customer_id").saveAsTable(...)` instead — N fixed buckets, no cardinality explosion.

**A9.** Parquet advantages over CSV:
1. **Columnar format** — only reads columns needed for a query (column pruning), skips the rest
2. **Embedded schema** — no parsing needed, type-safe, no `inferSchema` required
3. **Built-in compression** — Snappy/GZIP/ZSTD reduces file size 5-10× vs raw CSV
4. **Predicate pushdown** — reads only row groups matching filters (row group statistics)
5. **Splittable** — multiple tasks can read one file in parallel (unlike GZIP CSV)

**A10.**
```python
from pyspark.sql.types import *
schema = StructType([
    StructField("user_id",     StringType(),                  nullable=False),
    StructField("age",         IntegerType(),                 nullable=True),
    StructField("signup_date", DateType(),                    nullable=True),
    StructField("preferences", MapType(StringType(), StringType()), nullable=True),
])
```

**A11.** Use `mergeSchema=True` when reading:
```python
spark.read.option("mergeSchema", "true").json("/data/events")
```
Columns that exist in some records but not others appear with `null` for records that don't have them. All 7 fields appear in the unified schema; records with only 5 fields have null for the other 2.

**A12.**
- `cast()` fails silently → value becomes `null`. **Appropriate when**: you expect some values to be unparseable (mixed-type fields), and null is an acceptable representation of "unknown/invalid". Allows the pipeline to continue processing.
- `FAILFAST` → exception on first bad row. **Appropriate when**: ANY type mismatch means the data source is fundamentally broken (wrong schema version, wrong feed). You'd rather fail loud and fix the root cause than silently corrupt downstream analytics.

**A13.**
```python
df.select(
    "order_id",
    F.col("address.city").alias("address_city"),
    F.col("address.country").alias("address_country")
)
# Or with the flatten utility:
def flatten_struct(df):
    cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            for sub in field.dataType.fields:
                cols.append(F.col(f"{field.name}.{sub.name}").alias(f"{field.name}_{sub.name}"))
        else:
            cols.append(F.col(field.name))
    return df.select(cols)
```

**A14.**
```python
df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).show()
```
This is a single aggregation across all columns — one Spark job, not N jobs.

**A15.** Dead letter pattern: write invalid records to a separate table/path with:
1. `_original_data` or all source columns (preserve the raw data as-is)
2. `_error_reasons` — list of validation rules that failed (actionable for fixing)
3. `_processed_at` timestamp — when the record was rejected (for auditing and re-processing)

**A16.** `dropDuplicates(["order_id"])` keeps an arbitrary record when there are duplicates — Spark picks whichever partition the row appears in first, which is not deterministic across runs. `row_number()` ordered by `updated_at DESC` deterministically keeps the LATEST update. If a status update (pending → delivered) arrives and you keep the old row with `dropDuplicates`, the order is forever stuck as "pending" in your system — incorrect data in production.

**A17.** Three responses to 15% null `customer_id`:
1. **Accept**: if nulls are expected (guest orders, anonymous users) — add `customer_id IS NULL` handling in downstream logic
2. **Reject**: route null-customer-id rows to dead letter table, alert ops team, investigate source
3. **Enrich**: if there's a lookup table or another column that can be used to fill the null (e.g., `session_id → customer_id` join) — enrich before the quality check

**A18.** Yes, this is a problem — it causes **double-processing**. The next run loads the same watermark from before the crash → processes the same records again → appends duplicates to the output. This is the "dual-write" problem. Fix: use a database transaction that atomically (1) writes records AND (2) updates the watermark. Or use Delta Lake MERGE which is a single atomic operation.

**A19.**
- **Watermark-based**: filter source by `modified_at > last_watermark`. Best for: tables with a reliable `updated_at` column, streaming-like CDC feeds, irregular update patterns.
- **Partition-based**: load a fixed date partition (e.g., `dt=2024-01-17`). Best for: daily batch ETL where data is naturally organized by date, source is partitioned by day.
Partition-based is simpler and more robust (no watermark state to manage), but requires the source to be organized by date.

**A20.** `write.mode("append")` only adds new rows — it has no concept of "replace this existing row." For upsert, you need to: (a) insert new records AND (b) update existing records with new values. `append` would create a second copy of the updated row alongside the old one, giving you duplicates with different values. You'd then need to deduplicate on read, which is expensive. Delta Lake MERGE atomically handles both cases in one operation.

</details>
