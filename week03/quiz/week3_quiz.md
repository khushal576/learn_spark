# Week 3 Quiz — DataFrames & Spark SQL (Part 1)

Test yourself before moving to Week 4. Cover the answer with your hand, answer out loud, then check.

---

## Section 1: DataFrames vs RDDs (Notebook 01)

**Q1.** What are the three things a DataFrame has that a raw RDD does not?

<details><summary>Answer</summary>

1. **Schema** — named columns with data types
2. **Catalyst optimizer** — query planning, predicate pushdown, constant folding
3. **Tungsten engine** — off-heap binary format, code generation, vectorized execution

</details>

---

**Q2.** When you call `.rdd` on a DataFrame to get an RDD back, what type does each row become?

<details><summary>Answer</summary>

Each row becomes a `pyspark.sql.types.Row` object (a named tuple). You access fields as `row.col_name` or `row["col_name"]`, not by index (unless you call `row[i]`).

</details>

---

**Q3.** You have `df = spark.read.csv("file.csv")`. Which two methods give you the column names and their types?

<details><summary>Answer</summary>

- `df.printSchema()` — prints a tree of column names and types (readable)
- `df.dtypes` — returns a list of `(col_name, type_string)` tuples (programmatic)

</details>

---

**Q4.** What is the difference between `df.show()` and `df.collect()`?

<details><summary>Answer</summary>

- `df.show(n)` prints the first `n` rows to console — data stays in the cluster, only a small sample crosses to the Driver. Safe on large data.
- `df.collect()` pulls ALL rows to the Driver as a Python list of Row objects. Can cause OOM on large DataFrames. Use only when you know the dataset is small.

</details>

---

**Q5.** Name two ways to create a DataFrame from Python data (not from a file).

<details><summary>Answer</summary>

1. `spark.createDataFrame(list_of_tuples, schema=...)` — from a list with explicit schema
2. `spark.createDataFrame(pandas_df)` — from a pandas DataFrame
3. *(Bonus)* `spark.range(n)` — creates a DataFrame with a single `id` column

</details>

---

## Section 2: Schema & StructType (Notebook 02)

**Q6.** What is the difference between `inferSchema=True` and defining a `StructType` explicitly? Give one advantage of each.

<details><summary>Answer</summary>

- **inferSchema=True**: Spark samples the file to guess types — convenient but requires an extra data scan (slower) and may guess wrong (e.g., treats an ID column of `001` as integer → loses leading zeros).
- **Explicit StructType**: You define exact types upfront — no extra scan, 100% predictable types, self-documenting. Always use for production pipelines.

</details>

---

**Q7.** What does `nullable=True` mean in a `StructField`? What happens if you mark a column `nullable=False` but the data contains nulls?

<details><summary>Answer</summary>

`nullable=True` means the column is allowed to contain `null` values. `nullable=False` is a **hint to Catalyst** for optimization — it does NOT enforce the constraint at runtime. Spark will not throw an error; it just won't add null-checking code. The actual validation must happen in your pipeline logic.

</details>

---

**Q8.** You read a CSV file and all columns come in as `StringType`. How do you convert the `salary` column to `DoubleType` and `hire_date` to `DateType`?

<details><summary>Answer</summary>

```python
from pyspark.sql.functions import col, to_date

df = df \
    .withColumn("salary",    col("salary").cast("double")) \
    .withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd"))
```

Or using `StructType` when reading:
```python
schema = StructType([
    StructField("salary",    DoubleType(), True),
    StructField("hire_date", DateType(),   True),
    ...
])
df = spark.read.schema(schema).csv("file.csv")
```

</details>

---

**Q9.** What are `ArrayType`, `MapType`, and `StructType` (nested)? Give a real-world example of each.

<details><summary>Answer</summary>

- **ArrayType**: A column holding a list of values of the same type. Example: `tags: ["spark", "python", "bigdata"]`
- **MapType**: A column holding key-value pairs. Example: `metadata: {"region": "US", "tier": "premium"}`
- **Nested StructType**: A column containing a struct (sub-object). Example: `address: {street: "123 Main", city: "Austin", zip: "78701"}`

</details>

---

**Q10.** How do you handle null values in a DataFrame? Name two methods.

<details><summary>Answer</summary>

1. `df.dropna(how="any", subset=["col1", "col2"])` — drop rows where specified columns have nulls
2. `df.fillna({"salary": 0, "dept": "Unknown"})` — fill nulls per column with defaults
3. *(Bonus)* `df.na.replace({"old_val": "new_val"}, subset=["col"])` — replace specific values

</details>

---

## Section 3: Reading File Formats (Notebook 03)

**Q11.** Why is Parquet preferred over CSV for analytical workloads? Name three reasons.

<details><summary>Answer</summary>

1. **Columnar storage** — read only the columns you need (column pruning), not entire rows
2. **Predicate pushdown** — min/max statistics per row group let Spark skip irrelevant row groups without reading them
3. **Built-in compression** — Snappy/GZIP encoding on columns with repetitive values → much smaller files than CSV
4. *(Bonus)* **Schema embedded** — no need for inferSchema scan; types are guaranteed

</details>

---

**Q12.** What is partition pruning and how does it work with Parquet?

<details><summary>Answer</summary>

When you write Parquet with `partitionBy("year", "country")`, data is stored in directory paths like `year=2023/country=US/part-0.parquet`. When you filter on `year=2023`, Spark's file listing only includes the `year=2023/` directory — files from other years are never opened. This is partition pruning — entire directories skipped before any bytes are read.

</details>

---

**Q13.** What does `mode="PERMISSIVE"` do when reading a malformed CSV row?

<details><summary>Answer</summary>

In PERMISSIVE mode (the default), Spark puts the unparseable row into a special column called `_corrupt_record` and fills all other columns with `null`. The read does NOT fail. Use `badRecordsPath` to capture these rows for review.

Alternative modes:
- `DROPMALFORMED` — silently drops the bad row
- `FAILFAST` — throws an exception immediately

</details>

---

**Q14.** You write a DataFrame as Parquet and then read it back with a filter `df.filter(col("year") == 2023)`. How does Spark optimize this without reading all the data?

<details><summary>Answer</summary>

Parquet stores **column statistics** (min/max values) per row group (~128 MB chunks). When the filter `year == 2023` is pushed down, Spark checks each row group's min/max for the `year` column. If the entire row group has `year` values between 2019–2022, Spark skips it without deserializing any data. This is **predicate pushdown**. Visible in `explain()` as `PushedFilters`.

</details>

---

**Q15.** What are the four write modes? When would you use `append` vs `overwrite`?

<details><summary>Answer</summary>

| Mode | Behavior |
|---|---|
| `overwrite` | Delete existing data, write new |
| `append` | Add new data alongside existing |
| `error` (default) | Fail if data already exists |
| `ignore` | Skip write if data exists (no error) |

Use **overwrite** for full refreshes (daily snapshots, reprocessing). Use **append** for incremental loads (streaming output, daily new records added to history table).

</details>

---

## Section 4: Select, Filter, withColumn (Notebook 04)

**Q16.** What are three ways to reference a column in the DataFrame API?

<details><summary>Answer</summary>

1. `col("salary")` — from `pyspark.sql.functions`, works across DataFrames
2. `df["salary"]` — subscript syntax, tied to a specific DataFrame (safer with joins)
3. `"salary"` — string name, works in `select()`, `groupBy()`, `orderBy()` but not in expressions
4. *(Bonus)* `expr("salary * 1.1")` — SQL string expression via `expr()`

</details>

---

**Q17.** What is the difference between `select()` and `withColumn()`?

<details><summary>Answer</summary>

- `select()` produces a **new DataFrame with only the columns you specify**. It can rename, reorder, drop columns by omission, and apply expressions. Think of it as SELECT in SQL.
- `withColumn()` **adds or replaces one column while keeping all others**. Think of it as: `SELECT *, new_expr AS col_name FROM table`. Use `withColumn` when you want to add/update one column without listing every other column.

</details>

---

**Q18.** You want to filter rows where `dept` is either "Engineering" or "HR". Write it two ways.

<details><summary>Answer</summary>

```python
# Way 1: isin()
df.filter(col("dept").isin("Engineering", "HR"))

# Way 2: OR condition
df.filter((col("dept") == "Engineering") | (col("dept") == "HR"))

# Way 3: SQL string
df.filter("dept IN ('Engineering', 'HR')")
```

</details>

---

**Q19.** Why does `df.filter(col("salary") == None)` return 0 rows even when null values exist?

<details><summary>Answer</summary>

In Spark (and SQL), `null == null` evaluates to `null` (unknown), not `True`. The filter keeps rows where the expression evaluates to `True` — null is treated as non-matching. To filter nulls, use `col("salary").isNull()` or the SQL `salary IS NULL`.

</details>

---

**Q20.** What does `when(condition, value).when(cond2, val2).otherwise(default)` compile to in SQL?

<details><summary>Answer</summary>

It compiles to a `CASE WHEN ... THEN ... WHEN ... THEN ... ELSE ... END` expression. This is the Spark DataFrame equivalent of `CASE WHEN` in SQL. Catalyst generates identical bytecode for both the DataFrame API version and the `expr("CASE WHEN ...")` version.

</details>

---

**Q21.** What does `selectExpr()` do? When is it more useful than `select()`?

<details><summary>Answer</summary>

`selectExpr()` takes SQL expression strings and parses them through the Catalyst SQL parser — equivalent to `select(expr(...))`. It's more useful when:
1. You need SQL functions not directly available as Python functions: `"YEAR(hire_date) AS hire_year"`
2. You want to write multiple transformations inline in SQL syntax for readability
3. Porting SQL queries to DataFrame API — paste the SELECT clause expressions directly

```python
df.selectExpr("UPPER(name) AS name", "salary * 1.1 AS bumped_salary", "YEAR(hire_date) AS hire_year")
```

</details>

---

## Section 5: GroupBy & Aggregations (Notebook 05)

**Q22.** What is the difference between `count("*")` and `count("salary")` in a groupBy?

<details><summary>Answer</summary>

- `count("*")` counts **all rows** in the group, including rows where `salary` is null.
- `count("salary")` counts only rows where `salary` is **not null**.

This distinction is critical when checking for null completeness or when computing ratios.

</details>

---

**Q23.** You need to find the number of distinct customers per region on a 500M-row table. `countDistinct` is taking too long. What alternative do you use and what's the tradeoff?

<details><summary>Answer</summary>

Use `approx_count_distinct("customer_id", rsd=0.05)`. It uses HyperLogLog — a probabilistic data structure that uses O(1) memory regardless of cardinality, with a configurable relative standard deviation (rsd=0.05 means ~5% error). The tradeoff: you get an approximation, not an exact count. For dashboards, monitoring, and capacity planning, this accuracy is usually sufficient.

</details>

---

**Q24.** What is the difference between `rollup` and `cube`? Which one would you use for a time-hierarchy report (year → month → day)?

<details><summary>Answer</summary>

- `rollup(A, B, C)` computes subtotals for: (A,B,C), (A,B), (A), (grand total) — strictly hierarchical left-to-right.
- `cube(A, B, C)` computes ALL combinations: (A,B,C), (A,B), (A,C), (B,C), (A), (B), (C), (grand total).

For a time hierarchy (year → month → day), use **rollup** — it matches the natural left-to-right hierarchy and doesn't generate nonsensical subtotals like "all years in January" (`year=null, month=January`).

</details>

---

**Q25.** How do you do a HAVING clause equivalent in the DataFrame API?

<details><summary>Answer</summary>

Chain `.filter()` (or `.where()`) **after** `.agg()`:

```python
df.groupBy("dept")
  .agg(count("*").alias("headcount"), avg("salary").alias("avg_sal"))
  .filter(col("headcount") >= 3)   # ← this is the HAVING equivalent
  .orderBy("avg_sal")
```

The `.filter()` after `.agg()` runs on the aggregated result, which is exactly what `HAVING` does in SQL.

</details>

---

## Bonus Interview Scenarios

**Scenario 1:** Your Spark job reads a 100 GB CSV file and runs `groupBy("user_id").agg(count("*"))`. The job is slow and you notice in the Spark UI that one task takes 10x longer than all others. What's the problem and how do you fix it?

<details><summary>Answer</summary>

**Problem: Data skew.** One `user_id` (e.g., a bot or test account) has a disproportionately large number of rows. All rows for that key go to a single executor partition after the shuffle → that one task is the straggler.

**Fixes:**
1. **Salting**: Add a random suffix to the key before grouping, do a partial count, then aggregate the partial counts:
   ```python
   from pyspark.sql.functions import concat_ws, floor, rand
   df.withColumn("salted_key", concat_ws("_", col("user_id"), (floor(rand() * 10)).cast("string"))) \
     .groupBy("salted_key").agg(count("*").alias("partial_count")) \
     .withColumn("user_id", F.split(col("salted_key"), "_")[0]) \
     .groupBy("user_id").agg(sum("partial_count").alias("total_count"))
   ```
2. **Filter out the skewed keys** and process them separately.
3. **AQE (Adaptive Query Execution)**: `spark.sql.adaptive.enabled=true` — auto-splits skewed partitions in Spark 3.x.

</details>

---

**Scenario 2:** You need to produce a report that shows total sales for each (region, product_category) combination, plus subtotals for each region alone, plus a grand total. Which Spark feature do you use and why not the alternatives?

<details><summary>Answer</summary>

Use `rollup("region", "product_category")`. It produces:
- `(region, category)` — leaf level
- `(region, null)` — region subtotal
- `(null, null)` — grand total

Why not `cube`? `cube` would also produce `(null, category)` — a subtotal for each category across all regions. If that's not needed in the report, it's wasted computation and confusing output.

Why not multiple separate `groupBy` calls? You'd need 3 queries and a `union`, which reads the data 3 times. `rollup` does it in one pass with one shuffle.

</details>

---

**Scenario 3:** A colleague asks you to review their code. They wrote:
```python
result = df.groupBy("dept").agg(collect_list("transaction_id"))
```
The `transactions` table has 50M rows and some departments have millions of transactions. What's the risk?

<details><summary>Answer</summary>

**Risk: Driver/Executor OOM.** `collect_list` accumulates **all** values for a group into a single array. If the Engineering department has 5M transaction IDs, that's one array of 5M strings on a single executor partition. If multiple large groups exist, multiple executors run out of heap memory simultaneously.

**When it's safe:** Only use `collect_list` when you know groups are small (< tens of thousands of items).

**Alternatives:**
- If you need counts: use `count()` instead.
- If you need to write out all transactions per dept: use `partitionBy("dept")` when writing.
- If you need a sample: use `collect_list` after `limit()` within each group (via Window functions).
- If you just need unique values: consider `approx_count_distinct` or writing the grouped data to Parquet for downstream processing.

</details>
