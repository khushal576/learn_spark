# Week 3 — Phase 3: DataFrames & Spark SQL (Part 1)

## Prerequisites

- Week 1: Spark architecture, DAG, lazy evaluation, transformations vs actions
- Week 2: RDDs, Pair RDDs, shuffle internals, data skew

## Notebooks

| # | Notebook | Topics Covered |
|---|---|---|
| 01 | [01_dataframe_vs_rdd.ipynb](01_dataframe_vs_rdd.ipynb) | What DataFrames add over RDDs, Catalyst optimizer, Tungsten engine, 4 creation methods, inspection, SQL temp views |
| 02 | [02_schema_structtype.ipynb](02_schema_structtype.ipynb) | All Spark data types, inferSchema vs explicit StructType, complex types (Array/Map/Struct), nullable, null handling |
| 03 | [03_reading_file_formats.ipynb](03_reading_file_formats.ipynb) | Row vs columnar, CSV/JSON/Parquet/ORC read & write, predicate pushdown, partition pruning, write modes |
| 04 | [04_select_filter_withcolumn.ipynb](04_select_filter_withcolumn.ipynb) | col/expr/lit, when/otherwise, select/selectExpr, filter/where, withColumn, string functions, cast |
| 05 | [05_groupby_aggregations.ipynb](05_groupby_aggregations.ipynb) | groupBy+agg, all agg functions, pivot, rollup, cube, HAVING equivalent, shuffle cost |

## Sample Data

Located in [data/](data/):
- `employees.csv` — 10 rows, used in notebooks 02–04
- `products.json` — 5 rows NDJSON, used in notebook 03

---

## Week 3 Cheat Sheet

### Column References
```python
col("name")          # from pyspark.sql.functions — preferred
df["name"]           # tied to df — use in self-joins to disambiguate
expr("name || ' ' || dept")  # SQL string parsed by Catalyst
"name"               # string — works in select/groupBy/orderBy, not expressions
```

### Select Patterns
```python
df.select("col1", "col2")                    # by name
df.select(col("col1"), col("col2") * 1.1)    # with expressions
df.select("*", col("salary") * 12 .alias("annual"))  # keep all + add one
df.selectExpr("UPPER(name) AS name", "salary * 12 AS annual")
```

### Filter Patterns
```python
df.filter(col("salary") > 80000)
df.filter((col("dept") == "Eng") & (col("country") == "US"))
df.filter(col("dept").isin("Eng", "HR"))
df.filter(col("name").startsWith("A"))
df.filter(col("name").rlike("^[A-M].*"))    # regex
df.filter(col("salary").between(60000, 90000))
df.filter(col("salary").isNull())           # NOT: col("salary") == None
df.filter("salary > 80000")                 # SQL string (quick & dirty)
```

### withColumn Patterns
```python
df.withColumn("annual_salary", col("salary") * 12)
df.withColumn("grade",
    when(col("salary") > 90000, "Senior")
    .when(col("salary") > 70000, "Mid")
    .otherwise("Junior")
)
df.withColumn("name", F.upper(col("name")))  # replace existing column
df.withColumnRenamed("old_name", "new_name")
```

### Schema
```python
from pyspark.sql.types import *

schema = StructType([
    StructField("id",     IntegerType(), False),
    StructField("name",   StringType(),  False),
    StructField("salary", DoubleType(),  True),   # nullable
])

df = spark.read.schema(schema).csv("file.csv", header=True)
df.schema.json()   # serialize
StructType.fromJson(json.loads(json_str))  # deserialize
```

### Reading Files
```python
# CSV
spark.read.option("header", True).option("inferSchema", True).csv("path/")
spark.read.schema(schema).option("header", True).option("mode", "PERMISSIVE").csv("path/")

# JSON (NDJSON)
spark.read.json("path/")
spark.read.option("multiLine", True).json("path/")  # for pretty-printed JSON

# Parquet
spark.read.parquet("path/")

# ORC
spark.read.orc("path/")
```

### Writing Files
```python
df.write.mode("overwrite").parquet("output/")
df.write.mode("append").partitionBy("year", "country").parquet("output/")
df.write.mode("overwrite").option("header", True).csv("output/")
df.coalesce(1).write.mode("overwrite").parquet("single_file/")  # 1 file output
```

### Aggregations
```python
from pyspark.sql.functions import count, avg, sum, min, max
from pyspark.sql.functions import countDistinct, approx_count_distinct
from pyspark.sql.functions import collect_list, collect_set, stddev

df.groupBy("dept").agg(
    count("*").alias("n"),               # all rows
    count("salary").alias("non_null"),   # non-null only
    avg("salary").alias("avg_sal"),
    countDistinct("country").alias("countries"),
).filter(col("n") >= 3)                 # HAVING equivalent

# Pivot (always provide values list in production)
df.groupBy("dept").pivot("country", ["IN", "UK", "US"]).agg(avg("salary"))

# Rollup (hierarchical subtotals)
df.rollup("dept", "country").agg(count("*"), avg("salary"))

# Cube (all combinations)
df.cube("dept", "country").agg(count("*"), avg("salary"))
```

### Null Handling
```python
df.dropna(how="any", subset=["salary", "dept"])
df.fillna({"salary": 0.0, "dept": "Unknown"})
df.filter(col("salary").isNull())
df.filter(col("salary").isNotNull())
```

---

## Key Rules to Remember

1. **Always define schema explicitly** for production pipelines — `inferSchema` costs an extra scan and can mistype columns.
2. **Use Parquet** for intermediate storage — column pruning + predicate pushdown = huge speedup for analytical queries.
3. **`count("*")` includes nulls; `count("col")` skips nulls** — know which one you need.
4. **Null equality is broken**: `col("x") == None` always evaluates to null (not True). Use `.isNull()`.
5. **`pivot` without values list** runs an extra job — always provide the values when you know them.
6. **`collect_list` on large groups = OOM** — only use when groups are small.
7. **`groupBy` is a wide transformation** — triggers shuffle. Set `spark.sql.shuffle.partitions` appropriately for your data size (default 200 is too high for small data).
8. **`withColumn` vs `select`**: use `withColumn` to add/update one column; use `select` to reshape the whole DataFrame.

---

## Interview Questions — Week 3 Topics

1. Why are DataFrames faster than raw RDDs for most workloads?
2. What's the difference between `inferSchema=True` and defining a StructType? When would you use each?
3. Why is Parquet preferred over CSV for analytical workloads?
4. Explain predicate pushdown. How does it appear in `explain()`?
5. What is partition pruning and how does `partitionBy()` enable it?
6. What's the difference between `select()` and `withColumn()`?
7. Why does `df.filter(col("salary") == None)` not work for null filtering?
8. What are the four write modes and when would you use `append` vs `overwrite`?
9. Explain `when().when().otherwise()` — what SQL construct does it map to?
10. What is the difference between `count("*")` and `count("salary")` inside `agg()`?
11. How do you do a `HAVING` clause in the DataFrame API?
12. What is `pivot()` and what happens if you don't provide the values list?
13. What's the difference between `rollup` and `cube`? When would you use each?
14. When is `approx_count_distinct` preferred over `countDistinct`?

---

## Next Week

**Week 4 — Phase 3 (Part 2): Spark SQL & Joins**
- Registering and querying Spark SQL tables
- All join types: inner, left, right, full outer, cross, semi, anti
- Join strategies: broadcast hash join, sort-merge join, shuffle hash join
- Join optimizations: broadcast hints, AQE, eliminating skew in joins
- See [`../week4/README.md`](../week4/README.md)
