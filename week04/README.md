# Week 4: DataFrames & Spark SQL — Part 2

> **Phase 3 → Week 4**
>
> Prerequisites: Week 3 complete (DataFrames Part 1: schema, reading files, select/filter, groupBy)

---

## What You'll Learn This Week

Week 4 goes deeper into the DataFrame API — joins, analytical queries with window functions, the full suite of built-in functions, correct null handling, and working with real-world nested/complex data.

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [Join Types](01_join_types.ipynb) | inner/left/right/full/semi/anti/cross joins, broadcast, skew salting |
| 02 | [Window Functions](02_window_functions.ipynb) | row_number, rank, dense_rank, lag, lead, running totals, rolling avg |
| 03 | [Built-in Functions](03_builtin_functions.ipynb) | string, date, math, conditional (when/coalesce) |
| 04 | [Null Handling](04_null_handling.ipynb) | dropna, fillna, isNull, null in joins/aggregations |
| 05 | [Complex Types](05_complex_types.ipynb) | ArrayType, MapType, StructType, explode, flatten nested JSON |

---

## Data Files

```
week4/data/
├── customers.csv   — 10 customers (id, name, city, country, signup_date, tier)
├── orders.csv      — 20 orders (order_id, customer_id, product_id, quantity, amount, status)
├── products.csv    — 10 products (product_id, name, category, price, stock)
└── sales.csv       — 24 rows (salesperson, region, month, year, revenue, units_sold)
```

---

## Key Rules to Remember

### Joins
- Prefer **broadcast join** for small tables (<10MB) — eliminates shuffle
- Use **LEFT SEMI** when checking existence (not duplicating columns)
- Use **LEFT ANTI** for "not in" logic
- Null keys **never match** in any join type

### Window Functions
- Always define: `PARTITION BY` + `ORDER BY` (+ optional FRAME)
- `row_number()` for deduplication (exactly 1 row per group)
- `rank()` gaps after ties; `dense_rank()` never gaps
- Running total: `rowsBetween(unboundedPreceding, currentRow)`

### Built-in Functions
- Always prefer `pyspark.sql.functions` over Python UDFs — 10-100x faster
- `when().otherwise()` for conditional logic (CASE WHEN equivalent)
- `coalesce()` for null fallbacks

### Nulls
- `count(*)` ≠ `count(col)` when nulls exist — classic interview trap
- `col == None` **doesn't work** — use `col.isNull()`
- Nulls never match in joins — use `fillna` or filter before joining on nullable keys

### Complex Types
- `explode()` = one row per array element (drops empty/null arrays)
- `explode_outer()` = keeps rows with empty/null arrays
- Access struct fields with dot notation: `"address.city"`

---

## Interview Questions Covered

1. What is a broadcast join? When should you use it?
2. What's the difference between rank(), dense_rank(), and row_number()?
3. How do you get the top-N rows per group?
4. How do lag() and lead() work?
5. Why are built-in functions faster than Python UDFs?
6. What's the difference between `count(*)` and `count(col)`?
7. How do you check for null in a Spark filter?
8. What does `explode()` do?
9. How do you flatten a nested JSON struct?
10. What is data skew and how do you fix it with salting?

---

## How to Run

```bash
# Start cluster
docker-compose up -d

# Open Jupyter at http://localhost:8888
# Navigate to week4/ and run notebooks in order
```
