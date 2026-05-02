# Week 4 Quiz — DataFrames & Spark SQL Part 2

> Answer without looking at the notebooks. Check answers afterward.

---

## Section 1: Joins (10 points)

**Q1.** You have a `transactions` table (10M rows) and a `currency_codes` lookup table (50 rows). Which join strategy should Spark use, and how do you force it?

**Q2.** What's the result of this join?
```python
left  = [(1, "A"), (2, "B"), (None, "C")]   # (id, name)
right = [(1, "X"), (None, "Y"), (3, "Z")]   # (id, dept)
left.join(right, on="id", how="inner")
```
How many rows does the result have? Which rows are included?

**Q3.** Rewrite this using the most efficient join type:
```python
# Find all users who have made at least one purchase
users.join(purchases, on="user_id", how="inner").select("user_id", "name").distinct()
```

**Q4.** What's the difference between `left_semi` and `inner` join when the right side has duplicate keys?

**Q5.** Describe the salting technique for fixing data skew in a join. Why does it work?

---

## Section 2: Window Functions (10 points)

**Q6.** Write the code to rank employees by salary within each department (highest salary = rank 1). Use `dense_rank`. Show only the top 2 per department.

**Q7.** What's wrong with this window spec?
```python
w = Window.partitionBy("dept")
df.withColumn("prev_salary", F.lag("salary", 1).over(w))
```

**Q8.** What is the difference between:
```python
# A
w1 = Window.partitionBy("dept").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# B
w2 = Window.partitionBy("dept")
```
When would you use each?

**Q9.** Using `lag()`, how would you compute month-over-month revenue growth percentage? Write the column expression.

**Q10.** What does `ntile(4)` do? What real-world problem does it solve?

---

## Section 3: Built-in Functions (5 points)

**Q11.** Write a single `select()` expression that:
- Extracts the domain from an email column (e.g., `"alice@gmail.com"` → `"gmail.com"`)
- Converts name to uppercase
- Calculates how many days since `signup_date`

**Q12.** What's the difference between `coalesce(col1, col2)` and `when(col1.isNull(), col2).otherwise(col1)`?

**Q13.** Write a conditional aggregation that counts how many orders are "delivered" vs "cancelled" per customer in a single `groupBy`.

---

## Section 4: Null Handling (5 points)

**Q14.** You run `df.groupBy("dept").agg(F.avg("salary"))`. Two employees in the "Sales" dept have null salaries. Does the null affect the average? How?

**Q15.** You want to drop rows where EITHER `name` OR `email` is null (i.e., keep rows only if BOTH are non-null). Write the code.

---

## Section 5: Complex Types (5 points)

**Q16.** You have a DataFrame with a `tags` column of type `ArrayType(StringType)`. Write code to find all rows where `tags` contains `"premium"`.

**Q17.** What's the difference between `explode()` and `explode_outer()`? Give an example of when you'd use each.

**Q18.** How do you access the `city` field from a nested struct column `address` in Spark?

---

## Answers

<details>
<summary>Click to reveal answers</summary>

**A1.** Broadcast join. The `currency_codes` table is tiny (50 rows). Force with `broadcast(currency_codes)` hint or set `spark.sql.autoBroadcastJoinThreshold` to a value > the table size. Eliminates shuffle entirely.

**A2.** 1 row. Only `id=1` matches on both sides. `None == None` is `null` (unknown) in SQL semantics, so the null rows never match each other.

**A3.** Use `left_semi` join:
```python
users.join(purchases, on="user_id", how="left_semi").select("user_id", "name")
```
No `.distinct()` needed — semi join never duplicates rows even if right side has multiple matches.

**A4.** With duplicate keys on the right, INNER join duplicates rows from the left (one per right match). LEFT SEMI returns each left row exactly once regardless of how many right matches exist.

**A5.** Add a random integer (0 to N-1) suffix to the skewed key on the large side. On the small lookup side, create N copies of each key (one per salt value). Now the large key is split across N partitions instead of all going to one executor. After joining, the salt is dropped. Works because data is now evenly distributed across partitions.

**A6.**
```python
w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
df.withColumn("dr", F.dense_rank().over(w)).filter(F.col("dr") <= 2).drop("dr")
```

**A7.** `lag()` requires an `ORDER BY` in the WindowSpec. Without it, Spark doesn't know which row is "previous." Add `.orderBy("salary")` or `.orderBy("date")` depending on the intent.

**A8.** `w1` with `rowsBetween(unboundedPreceding, currentRow)` is for running/cumulative aggregations (expanding window). `w2` without orderBy or frame is for partition-wide aggregations (same value for all rows in a partition — like showing each row with its dept average).

**A9.**
```python
F.round(
    (F.col("revenue") - F.lag("revenue", 1).over(w_person)) /
    F.lag("revenue", 1).over(w_person) * 100,
    2
).alias("mom_growth_pct")
```

**A10.** `ntile(4)` divides rows into 4 equal-size buckets (quartiles). Bucket 1 = bottom 25%, bucket 4 = top 25%. Use for percentile-based segmentation: "top quartile customers", "bottom 25% by performance".

**A11.**
```python
df.select(
    F.regexp_extract("email", r"@(.+)", 1).alias("domain"),
    F.upper("name").alias("name_upper"),
    F.datediff(F.current_date(), F.to_date("signup_date")).alias("days_since_signup")
)
```

**A12.** They produce identical results for simple cases. But `coalesce()` is shorter and can take N arguments. `when()` is more flexible — you can use any condition, not just null-check, and chain multiple conditions.

**A13.**
```python
df.groupBy("customer_id").agg(
    F.sum(F.when(F.col("status") == "delivered", 1).otherwise(0)).alias("delivered"),
    F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancelled")
)
```

**A14.** Nulls are silently excluded from `avg()`. The average is computed over only the non-null values. If 2 out of 5 Sales employees have null salary, avg uses only the 3 non-null values. `count("salary")` would return 3, not 5.

**A15.**
```python
df.filter(F.col("name").isNotNull() & F.col("email").isNotNull())
# or equivalently:
df.dropna(subset=["name", "email"])
```

**A16.**
```python
df.filter(F.array_contains(F.col("tags"), "premium"))
```

**A17.** `explode()` drops rows where the array is null or empty — only rows with ≥1 element are kept. `explode_outer()` keeps null/empty rows (with null in the exploded column). Use `explode()` when you only care about items. Use `explode_outer()` when you need to preserve records with no items (e.g., users with no purchases).

**A18.** `df.select("address.city")` or `F.col("address.city")`. For programmatic access: `F.col("address").getField("city")`.

</details>
