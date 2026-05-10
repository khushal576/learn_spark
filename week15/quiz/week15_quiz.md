# Week 15 Quiz — Testing, CI/CD & Data Quality

> Test yourself after completing all 5 notebooks. No notes. This is the final week — make it count.

---

## Section A: Unit Testing

**Q1.** Why should a `SparkSession` fixture use `scope='session'` instead of `scope='function'`? What are the performance implications?

<details><summary>Answer</summary>

`scope='function'` (default): creates and destroys a SparkSession for every test function. Starting a JVM + Spark context takes 3-10 seconds. With 50 tests, that's 150-500 seconds of overhead — CI would take 5-8 minutes just on setup/teardown.

`scope='session'`: one SparkSession is created when the first test runs and destroyed when the last test finishes. 3-10 second startup cost paid once. Tests themselves run in milliseconds.

Additional implication: `scope='session'` means all tests share the same Spark context. Tests must not permanently change SparkSession configuration (use `spark.conf.set()` carefully — reset after the test).
</details>

---

**Q2.** You have a `compute_revenue(df)` function. Write 4 test cases that cover: the happy path, an empty DataFrame, all-null amounts, and a single row.

<details><summary>Answer</summary>

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField('customer_id', StringType()),
    StructField('amount', DoubleType()),
    StructField('status', StringType()),
])

def test_happy_path(spark):
    df = spark.createDataFrame([
        ('C1', 100.0, 'shipped'), ('C1', 200.0, 'shipped'),
        ('C2',  50.0, 'shipped'),
    ], ['customer_id', 'amount', 'status'])
    result = compute_revenue(df)
    rows = {r.customer_id: r.revenue for r in result.collect()}
    assert rows['C1'] == 300.0
    assert rows['C2'] == 50.0

def test_empty_dataframe(spark):
    df = spark.createDataFrame([], schema)
    assert compute_revenue(df).count() == 0

def test_all_null_amounts(spark):
    df = spark.createDataFrame([('C1', None, 'shipped')], ['customer_id','amount','status'])
    result = compute_revenue(df)
    # depends on impl: NULL sum = NULL or 0
    # test documents the expected behavior
    rows = result.collect()
    assert len(rows) == 1

def test_single_row(spark):
    df = spark.createDataFrame([('C1', 75.0, 'shipped')], ['customer_id','amount','status'])
    result = compute_revenue(df)
    assert result.count() == 1
    assert result.collect()[0]['revenue'] == 75.0
```
</details>

---

**Q3.** What does `@pytest.mark.parametrize` do? Rewrite the tier boundary tests using it.

<details><summary>Answer</summary>

`@pytest.mark.parametrize` runs the same test function multiple times with different input/output pairs. Avoids copy-pasting test functions for each boundary value.

```python
import pytest

@pytest.mark.parametrize('amount, expected_tier', [
    (500.0, 'gold'),
    (600.0, 'gold'),
    (499.9, 'silver'),
    (200.0, 'silver'),
    (199.9, 'bronze'),
    (  1.0, 'bronze'),
])
def test_tier_boundaries(spark, amount, expected_tier):
    df = spark.createDataFrame([(amount,)], ['amount'])
    result = enrich_with_tier(df).collect()[0]['tier']
    assert result == expected_tier, f'{amount} → expected {expected_tier}, got {result}'
```

pytest generates 6 separate test cases with clear names: `test_tier_boundaries[500.0-gold]`, etc. On failure, you see exactly which boundary value failed.
</details>

---

**Q4.** What is `chispa.assert_df_equality()` and how does it improve over `result.collect() == expected.collect()`?

<details><summary>Answer</summary>

`result.collect() == expected.collect()` fails with an unhelpful `AssertionError: False`. You don't see which rows are different or what the actual vs expected values are.

`chispa.assert_df_equality(actual, expected, ignore_row_order=True)` shows a clear diff:
```
DataFramesNotEqualError: 
  Actual row:   Row(order_id='O1', amount=100.0, tier='bronze')
  Expected row: Row(order_id='O1', amount=100.0, tier='silver')
```

Key parameters:
- `ignore_row_order=True`: compares as sets, not positionally (Spark doesn't guarantee order)
- `ignore_nullable=True`: ignore nullable flag differences in schema
- `check_dtype=True` (default): fails if column types differ (float vs double)
</details>

---

## Section B: Data Quality

**Q5.** What are the five dimensions of data quality? Give one concrete check for each using Spark.

<details><summary>Answer</summary>

| Dimension | Spark check |
|-----------|-------------|
| **Completeness** | `df.filter(F.col('customer_id').isNull()).count() == 0` |
| **Validity** | `df.filter(~F.col('status').isin(['shipped','pending','cancelled'])).count() == 0` |
| **Uniqueness** | `df.count() == df.select('order_id').distinct().count()` |
| **Consistency** | `orders.join(customers, 'customer_id', 'left_anti').count() == 0` (referential integrity) |
| **Timeliness** | `(datetime.now() - df.agg(F.max('event_ts')).collect()[0][0]).total_seconds() / 3600 < 2` |
</details>

---

**Q6.** What is the dead-letter pattern? Write the PySpark code to split valid and invalid orders into separate DataFrames.

<details><summary>Answer</summary>

Dead-letter: route invalid records to a quarantine table with a `_rejection_reason` column, instead of discarding them.

```python
def split_valid_invalid(df):
    condition = (
        F.col('customer_id').isNotNull() &
        (F.col('amount') > 0) &
        F.col('status').isin('shipped', 'pending', 'cancelled')
    )
    rejection_reason = (
        F.when(F.col('customer_id').isNull(), F.lit('|null_customer')).otherwise(F.lit('')) +
        F.when(F.col('amount') <= 0,         F.lit('|bad_amount')).otherwise(F.lit('')) +
        F.when(~F.col('status').isin('shipped','pending','cancelled'), F.lit('|bad_status')).otherwise(F.lit(''))
    )
    tagged = df.withColumn('_rejection_reason', rejection_reason)
    valid       = tagged.filter(F.col('_rejection_reason') == '').drop('_rejection_reason')
    dead_letter = tagged.filter(F.col('_rejection_reason') != '') \
                        .withColumn('_rejected_at', F.current_timestamp())
    return valid, dead_letter
```
</details>

---

**Q7.** How do you detect distribution shift between today's data and a 7-day baseline? What threshold would you use for `mean` change?

<details><summary>Answer</summary>

Compute column statistics (mean, stddev, count) for today and the 7-day baseline. Compare:

```python
def check_shift(today_stats, baseline_stats, col, mean_tol=0.20):
    mean_shift = abs(today_stats['mean'] - baseline_stats['mean']) / baseline_stats['mean']
    if mean_shift > mean_tol:
        raise ValueError(f'{col} mean shifted {mean_shift:.0%} > {mean_tol:.0%}')
```

Typical thresholds: mean ± 20%, count ± 30%, stddev ± 50%. Tighter for critical financial columns (revenue), looser for behavioral metrics (session length). In practice, start loose (50%) and tighten once you understand seasonal variation in your data.

Store 7-day stats as JSON in S3 after each run, load them in the next run for comparison.
</details>

---

## Section C: Great Expectations

**Q8.** What is the difference between `@dlt.expect` (Delta Live Tables) and `expect_column_values_to_not_be_null` (Great Expectations)? When would you choose each?

<details><summary>Answer</summary>

**DLT `@dlt.expect`**: Databricks-only, declarative, built into the pipeline definition. Managed by DLT runtime — tracks metrics in the pipeline UI, integrates with DLT's auto-retry and dependency management. Only works in Databricks.

**GX `expect_column_values_to_not_be_null`**: framework-agnostic, works with any Spark/Pandas setup, generates Data Docs HTML reports, 300+ built-in expectations, integrates with Airflow via operator.

Choose DLT expectations: Databricks shop, want DLT's built-in pipeline UI to show DQ metrics, simple pass/drop/fail behavior.
Choose GX: non-Databricks (EMR, Glue, local), need detailed HTML reports to share with business stakeholders, want broader ecosystem of expectations and actions.
</details>

---

**Q9.** What does `mostly=0.95` mean in a GX expectation? Give a concrete scenario where you'd use it.

<details><summary>Answer</summary>

`mostly=0.95` means at least 95% of rows must satisfy the expectation. Up to 5% may fail and the overall expectation still passes.

Scenario: `expect_column_values_to_not_be_null('region', mostly=0.95)`. Your source system is a legacy CRM that sometimes doesn't record the region for very old accounts. You know from historical data that 2-3% of records legitimately have null region. With `mostly=0.95`, the expectation passes for up to 5% null, but alerts if it climbs above 5% (suggesting a new data issue, not the known legacy problem).

Never use `mostly` for `order_id` or other primary keys — those must be 100% non-null.
</details>

---

## Section D: CI/CD

**Q10.** Write a minimal GitHub Actions CI workflow that: runs on every PR to `main`, installs Python 3.11 + Java 17, runs `ruff check`, then `pytest tests/unit/` with `--cov-fail-under=80`.

<details><summary>Answer</summary>

```yaml
name: CI
on:
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - uses: actions/setup-java@v4
        with:
          java-version: '17'
          distribution: temurin

      - run: pip install -r requirements-dev.txt

      - name: Lint
        run: ruff check src/ tests/

      - name: Test
        run: pytest tests/unit/ -v --cov=src --cov-fail-under=80
```
</details>

---

**Q11.** What is GitHub OIDC authentication for AWS? Why is it better than storing `AWS_ACCESS_KEY_ID` in GitHub Secrets?

<details><summary>Answer</summary>

**Problem with stored keys**: `AWS_ACCESS_KEY_ID` is a long-lived credential. If it leaks (GitHub breach, accidentally logged, rogue employee), attackers have persistent access until manually rotated. Key rotation is manual and error-prone.

**OIDC**: GitHub acts as an Identity Provider. When a workflow runs, GitHub issues a short-lived OIDC token signed by GitHub. The workflow presents it to AWS STS, which verifies the token and returns temporary credentials (valid ~1 hour). No long-lived secrets stored anywhere.

Setup:
1. Create GitHub OIDC identity provider in AWS IAM
2. Create IAM Role with trust policy: `"StringLike": {"token.actions.githubusercontent.com:sub": "repo:myorg/myrepo:*"}`
3. Workflow uses `permissions: id-token: write` + `aws-actions/configure-aws-credentials@v4`

If credentials leak: they expire in 1 hour with no action required.
</details>

---

## Section E: Production Best Practices

**Q12.** What is structured logging and how does it differ from `print()` or basic `logging.info()`?

<details><summary>Answer</summary>

`print("Processing complete")` → unstructured text, impossible to query or aggregate.

`logging.info("Processing complete")` → has level + timestamp but still free text.

Structured logging: emit JSON lines where every field is a named key-value pair:
```json
{"ts": "2024-01-15T10:30:00Z", "level": "INFO", "pipeline": "orders_etl",
 "layer": "silver", "run_id": "run_001", "msg": "complete", "rows": 49800, "duration_s": 45.3}
```

Benefits: (1) CloudWatch Logs Insights can query `stats avg(duration_s) by pipeline`, (2) alert on `rows < 1000` using CloudWatch metric filters, (3) Datadog/Splunk can parse and aggregate without regex, (4) consistent format across all jobs in the org, (5) `run_id` lets you correlate all events from one execution.
</details>

---

**Q13.** What is the difference between storing config in environment variables vs a config dict vs AWS SSM Parameter Store? When would you use each?

<details><summary>Answer</summary>

**Environment variables**: set in the shell before running the job. Easy but: no type checking, easy to forget to set them, hard to audit what value was used for a historical run.

**Config dict in code** (`CONFIGS = {"dev": {...}, "prod": {...}}`): checked into version control, auditable, easy to review diffs. Best for: non-sensitive, structural config (paths, thresholds, parallelism). Checked in → every change is a PR.

**AWS SSM Parameter Store**: centralized, IAM-controlled, versioned, supports SecureString for sensitive values. Best for: values that change without a code deploy (feature flags, rate limits), secrets (use SecretsManager for highest-security), shared config across multiple services.

Combined pattern: code config dict for static structure + SSM for dynamic/sensitive values.
</details>

---

**Q14.** A team asks you to review their Spark pipeline before production launch. List the top 5 things you'd check and why each matters.

<details><summary>Answer</summary>

1. **Idempotency** — can they re-run safely after a failure? Check: does the write use `mode=overwrite` with dynamic partition, Delta MERGE, or another idempotent pattern? A non-idempotent pipeline creates duplicates on retry.

2. **Failure handling** — does the job exit with code 1 on exception? Does the Airflow cluster terminate on failure (`trigger_rule=all_done`)? Without this, silent failures and orphaned clusters accumulate.

3. **Data quality checks** — is there any DQ validation between layers? Without checks, bad data propagates silently to Gold and business dashboards.

4. **No hardcoded paths/credentials** — check for `s3://my-bucket/prod/...` hardcoded, or database passwords in the script. These break in different environments and are security risks.

5. **Tests** — does any test exist? Even one test that exercises the main transformation shows the code is structured for testability. Zero tests = high risk of regression on every change.
</details>
