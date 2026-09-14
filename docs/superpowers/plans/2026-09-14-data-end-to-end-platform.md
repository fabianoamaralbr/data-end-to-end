# Data End-to-End Platform — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-grade end-to-end Azure data platform ingesting Kaggle financial stock data, transforming via medallion lakehouse (Databricks), exposing analytics via Synapse SQL, and visualizing in Power BI — with full IaC, CI, tests and docs.

**Architecture:** CSV via ADF → ADLS raw → Databricks (bronze → silver → gold Delta Lake) → Synapse Serverless SQL external tables → Power BI DirectQuery.

**Tech Stack:** Azure Data Factory · ADLS Gen2 · Delta Lake · Azure Databricks PySpark · Azure Synapse Analytics Serverless SQL · Power BI PBIP · Terraform · pytest-spark · GitHub Actions

---

## File Map

| Path | Responsibility |
|------|---------------|
| `.gitignore` / `.env.example` | Secrets exclusion + env var docs |
| `requirements.txt` / `requirements-dev.txt` | Runtime + dev deps |
| `Makefile` | `make test`, `make lint`, `make deploy-infra` |
| `data/sample/financial_sample.csv` | 500-row OHLCV sample for local tests |
| `scripts/download_kaggle_data.py` | Kaggle API download helper |
| `adf/linked_service/*.json` | ADF connections (ADLS, HTTP) |
| `adf/dataset/*.json` | Source/sink dataset definitions |
| `adf/pipeline/pl_ingest_financial_data.json` | Full ingestion pipeline |
| `databricks/notebooks/00_setup_mounts.py` | ADLS → DBFS mounts |
| `databricks/notebooks/01_raw_to_bronze.py` | CSV → Delta bronze |
| `databricks/notebooks/02_bronze_to_silver.py` | Clean + enrich → Delta silver |
| `databricks/notebooks/03_silver_to_gold.py` | Aggregations → Delta gold |
| `databricks/jobs/medallion_workflow.json` | Multi-task Databricks workflow |
| `synapse/sql/00_setup_database.sql` | DB + managed identity credential |
| `synapse/sql/01_create_external_data_sources.sql` | ADLS external sources |
| `synapse/sql/02_create_external_tables.sql` | Delta Lake external tables |
| `synapse/sql/03_create_analytics_views.sql` | Business analytics views |
| `powerbi/financial_model.SemanticModel/model.bim` | Semantic model (tables, measures) |
| `powerbi/financial_dashboard.Report/report.json` | Report page definitions |
| `tests/conftest.py` | Local PySpark session fixture |
| `tests/unit/test_bronze.py` | Bronze transform unit tests |
| `tests/unit/test_silver.py` | Silver transform unit tests |
| `tests/unit/test_gold.py` | Gold aggregate unit tests |
| `infra/main.tf` + `variables.tf` + `outputs.tf` | Terraform root |
| `infra/modules/storage/main.tf` | ADLS + 4 containers |
| `infra/modules/adf/main.tf` | Data Factory |
| `infra/modules/databricks/main.tf` | Databricks workspace |
| `infra/modules/synapse/main.tf` | Synapse workspace |
| `.github/workflows/ci.yml` | Lint + unit tests on PR |
| `docs/architecture/architecture.md` | Mermaid diagram + ADRs |
| `README.md` | Impeccable README with README |

---

## Tasks

### Task 1 — Repository Scaffold
- [ ] `.gitignore`, `.env.example`, `requirements.txt`, `requirements-dev.txt`, `Makefile`
- [ ] `git commit -m "chore: repository scaffold"`

### Task 2 — Sample Data + Kaggle Script
- [ ] `data/sample/financial_sample.csv` (500 rows, AAPL/MSFT/GOOGL, 2020-2021)
- [ ] `scripts/download_kaggle_data.py`
- [ ] `git commit -m "chore: add sample financial data and kaggle download script"`

### Task 3 — Azure Data Factory Artifacts
- [ ] Linked services, datasets, pipeline JSON
- [ ] `git commit -m "feat(adf): ingestion pipeline for financial CSV from Kaggle"`

### Task 4 — Databricks Notebooks (medallion)
- [ ] 00_setup_mounts, 01_raw_to_bronze, 02_bronze_to_silver, 03_silver_to_gold
- [ ] `databricks/jobs/medallion_workflow.json`
- [ ] `git commit -m "feat(databricks): medallion pipeline bronze/silver/gold"`

### Task 5 — Unit Tests (TDD)
- [ ] `tests/conftest.py`, bronze/silver/gold test files
- [ ] Run `make test` → all green
- [ ] `git commit -m "test: unit tests for medallion transformations"`

### Task 6 — Synapse Analytics SQL
- [ ] Setup DB, external data sources, external tables, analytics views
- [ ] `git commit -m "feat(synapse): serverless SQL external tables and analytics views"`

### Task 7 — Power BI Project Files
- [ ] `model.bim`, `report.json`
- [ ] `git commit -m "feat(powerbi): semantic model and dashboard definition"`

### Task 8 — Terraform Infrastructure
- [ ] Root + 4 modules (storage, adf, databricks, synapse)
- [ ] `git commit -m "feat(infra): terraform for all azure resources"`

### Task 9 — GitHub Actions CI
- [ ] `.github/workflows/ci.yml`
- [ ] `git commit -m "ci: lint and test pipeline on PR"`

### Task 10 — Architecture Docs + README
- [ ] `docs/architecture/architecture.md` (Mermaid + ADRs)
- [ ] `README.md` with README
- [ ] `git commit -m "docs: architecture diagram, ADRs, and README"`
