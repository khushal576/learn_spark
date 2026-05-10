# Week 16: Phase 7 — Capstone Project

> **Phase 7 → Week 16 (Final Week)**
>
> Prerequisites: All Weeks 1-15

---

## What You'll Build This Week

Week 16 is a full end-to-end production pipeline for **ShopStream** — a fictional e-commerce company processing 500K orders/day. You apply every concept from the course in one integrated project.

| # | Notebook | Core Skill |
|---|----------|-----------|
| 01 | [System Design](01_capstone_design.ipynb) | Architecture decisions, technology matrix, cost estimation |
| 02 | [Bronze & Silver Layers](02_capstone_bronze_silver.ipynb) | Delta ingest, dead-letter, deduplication, DQ checks |
| 03 | [Gold Layer & Streaming](03_capstone_gold_streaming.ipynb) | Gold aggregation, Structured Streaming, windowed metrics |
| 04 | [Orchestration & CI/CD](04_capstone_orchestration_cicd.ipynb) | Full Airflow DAG, production Spark job, GitHub Actions |
| 05 | [Final Interview Prep](05_final_interview_prep.ipynb) | Complete Q&A reference across all 16 weeks |

---

## ShopStream Architecture

```
SOURCES
  PostgreSQL RDS (500K orders/day)   → DMS CDC → S3 Bronze (Delta, date-partitioned)
  Kafka MSK   (5M events/day)        → Spark Streaming → Delta streaming table
  SFTP vendor (hourly CSVs)          → Lambda → S3 Bronze

BATCH (daily 6am UTC, EMR + Airflow)
  Bronze validate → Silver transform+DQ → Gold aggregate → freshness check

SERVING
  Athena → S3 Gold (ad-hoc analyst SQL)
  QuickSight → SPICE (5-min refresh from streaming Delta table)
  SageMaker → reads Silver directly

COST: ~$3,080/month
```

---

## Key Rules Applied (All Weeks)

### Medallion Architecture
- **Bronze**: as-is, metadata columns (`_ingested_at`, `_source`, `_run_id`), never filtered
- **Silver**: validated, deduplicated, enriched; dead-letter for rejections; DQ checks before promoting
- **Gold**: one table = one business question; always includes `_updated_at` for freshness monitoring

### Production Patterns Applied
- Dynamic partition overwrite (`spark.sql.sources.partitionOverwriteMode=dynamic`) — idempotent writes
- Dead-letter with `_rejection_reason` — no data loss
- Structured JSON logging with `run_id` — queryable by CloudWatch
- `sys.exit(1)` on failure — EMR Step FAILED → Airflow detects
- EMR cluster terminates with `trigger_rule=ALL_DONE` — no orphaned clusters
- GitHub OIDC — no stored AWS credentials in CI
- pytest coverage ≥ 80% enforced in CI before deploy

### Technology Decisions
| Decision | Choice | Key Reason |
|----------|--------|-----------|
| Table format | Delta Lake | Best Spark integration, MERGE + CDF |
| Compute | EMR with Spot | 60-80% cost savings vs On-Demand |
| Orchestration | Airflow MWAA | Cross-system dependencies, team familiarity |
| Serving | Athena + Redshift Spectrum | Zero infra + complex BI queries |
| Streaming | Spark Structured Streaming | 5-min SLA, reuse EMR, Delta sink |

---

## Interview Questions Covered

1. Design a real-time analytics platform for e-commerce (full system design)
2. How does the Medallion Architecture work? What goes in each layer?
3. How do you handle a GDPR deletion request in a data lake?
4. What is idempotency and how do you achieve it in a batch pipeline?
5. Walk me through the Airflow DAG for a production Spark pipeline
6. Why terminate the EMR cluster with `trigger_rule=ALL_DONE`?
7. What is dynamic partition overwrite and why does it matter?
8. How do you make a Spark pipeline production-ready? (6 points)
9. Delta Lake vs Iceberg vs Hudi — when would you choose each?
10. How do you recover from a Gold table overwrite? (time travel)
11. What is the dead-letter pattern and why is it better than filtering?
12. How does GitHub OIDC authentication work?
13. What is Spark AQE and what three problems does it solve?
14. How do you debug a slow Spark job? (step-by-step process)
15. What is a watermark in Structured Streaming?
16. What is `foreachBatch` and when would you use it?
17. EMR vs Glue vs Databricks — when would you choose each?
18. How do you handle EMR cluster failures in Airflow?
19. What is S3 dynamic partition overwrite?
20. Walk me through designing the ShopStream data platform from scratch

---

## How to Run

```bash
docker-compose up -d
# Open http://localhost:8888
# Navigate to week16/ and run notebooks in order
# Notebooks 01-03, 05 run fully locally (no AWS required)
# Notebooks 04 are reference guides (Airflow/EMR patterns)
```
