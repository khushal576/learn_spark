# Week 15: Phase 7 — Testing, CI/CD & Data Quality

> **Phase 7 → Week 15**
>
> Prerequisites: Weeks 13-14 (cloud platforms, Airflow, AWS integration)

---

## What You'll Learn This Week

Week 15 covers the engineering practices that separate a reliable production pipeline from a fragile script: unit testing with pytest, a custom data quality framework, Great Expectations, CI/CD with GitHub Actions, and production best practices (logging, config, secrets, production readiness checklist).

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [Unit Testing PySpark](01_unit_testing_pyspark.ipynb) | pytest fixtures, session-scoped SparkSession, assertion helpers, parametrized tests |
| 02 | [Data Quality Framework](02_data_quality_framework.ipynb) | Custom DQ checks (null, unique, RI, distribution shift), dead-letter pattern, severity levels |
| 03 | [Great Expectations](03_great_expectations.ipynb) | Expectation Suites, 300+ built-in checks, mostly parameter, Airflow integration |
| 04 | [CI/CD with GitHub Actions](04_cicd_github_actions.ipynb) | ruff linting, pytest in CI, coverage gates, GitHub OIDC for AWS, deploy on merge |
| 05 | [Production Best Practices](05_production_best_practices.ipynb) | Structured logging, config management, secret management, production readiness checklist |

---

## Key Rules to Remember

### Unit Testing
- Session-scoped `SparkSession` in `conftest.py` — one JVM for the entire test run (not one per test)
- Test pure functions only: `validate(df)`, `deduplicate(df)`, `enrich(df)` — no I/O in unit tests
- Minimal test data: 3-10 rows covering the happy path + every edge case
- Use `@pytest.mark.parametrize` for boundary value testing (tier thresholds, date ranges)
- Run `pytest tests/unit/ --cov=src --cov-fail-under=80` in CI
- `chispa.assert_df_equality(actual, expected, ignore_row_order=True)` for clean DataFrame diffs

### Data Quality
- Five DQ dimensions: Completeness, Validity, Uniqueness, Consistency, Timeliness
- Dead-letter table: never discard bad records — quarantine them with `_rejection_reason`
- Severity levels: ERROR (fail the pipeline), WARN (alert but continue)
- Referential integrity: left anti-join — orphan count > 0 = violation
- Distribution shift: compare `mean`, `stddev`, `count` vs 7-day baseline; alert on > 20% shift
- Run DQ after Silver write — fail the Airflow task if critical rules violated

### Great Expectations
- Expectation Suite: named collection of assertions stored as JSON — reused across every run
- `mostly=0.99` means 99% of rows must pass (allows 1% tolerance)
- Key expectations: `not_be_null`, `be_unique`, `be_in_set`, `be_between`, `mean_to_be_between`
- GX Checkpoint: configured, re-runnable validation with actions (store results, update Data Docs, alert)
- Data Docs: auto-generated HTML report — shareable DQ status across teams

### CI/CD
- Every PR: `ruff check` → `pytest tests/unit/` → fail if any test fails or coverage < 80%
- ruff replaces flake8 + isort + pyflakes — 100× faster; `ruff check --fix` auto-corrects
- On merge to main: package code → upload to S3 → (optionally) trigger EMR/Databricks run
- GitHub OIDC: no stored AWS credentials — GitHub exchanges OIDC token for temporary AWS creds
- Quality gates: lint errors = CI fails, test failures = CI fails, coverage < threshold = CI fails

### Production Best Practices
- Structured logging: JSON lines with `ts, level, pipeline, layer, run_id, msg` — parseable by CloudWatch
- Config per env: `CONFIGS = {"dev": {...}, "staging": {...}, "prod": {...}}` — no hardcoded paths
- Secrets: AWS Secrets Manager (`get_secret_value`) at runtime — never in code, env vars, or spark args
- `sys.exit(1)` on failure: non-zero exit → EMR step FAILED → Airflow detects → retries/alerts
- Production readiness: testable + idempotent + observable + configurable + CI/CD = production-ready

---

## Interview Questions Covered

1. How do you unit test a PySpark transformation function?
2. What is a pytest fixture and why use `scope='session'` for SparkSession?
3. What is the difference between unit, integration, and end-to-end tests for Spark?
4. What is `chispa` and why is it better than manual `.collect()` assertions?
5. What are the five dimensions of data quality?
6. What is a dead-letter table and why is it better than dropping bad records?
7. How do you check referential integrity in Spark?
8. What is distribution shift monitoring and how do you implement it?
9. What is Great Expectations and how does it integrate with Spark/Airflow?
10. What is a GX Expectation Suite?
11. What does the `mostly` parameter in GX do?
12. What is a GX Checkpoint vs calling `validate()` directly?
13. What is `ruff` and why should you use it instead of flake8?
14. How do you implement CI/CD for a Spark pipeline with GitHub Actions?
15. What is GitHub OIDC authentication and why is it safer than stored AWS keys?
16. What is `--cov-fail-under` and how does it enforce coverage in CI?
17. How do you manage environment-specific configuration (dev/staging/prod) in Spark?
18. How do you handle secrets (DB passwords, API keys) in a Spark pipeline?
19. What is structured logging and why is it better than plain `print()` for production?
20. What makes a Spark pipeline production-ready? (6-point answer)

---

## How to Run

```bash
docker-compose up -d
# Open http://localhost:8888
# Navigate to week15/ and run notebooks in order
# All notebooks run fully locally (no AWS required)
# For pytest: docker-compose exec jupyter pytest /workspace/week15/tests/ -v
```
