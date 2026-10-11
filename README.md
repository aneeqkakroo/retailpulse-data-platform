# RetailPulse Data Platform

RetailPulse is an end-to-end retail data engineering platform built to demonstrate modern batch data engineering practices across local and Azure environments.

## Architecture

```text
PostgreSQL
    ↓
Incremental Bronze ingestion
    ↓
ADLS Gen2
    ↓
Azure Databricks
    ↓
Delta Silver + Quarantine
    ↓
Gold dimensional model
    ↓
Unity Catalog
    ↓
Power BI
```

Supporting capabilities include Terraform, GitHub Actions, Databricks Jobs, automated data-quality checks, pipeline audit logging, and local Docker-based development.

## Key Features

- Incremental PostgreSQL ingestion using `updated_at` watermarks
- Partitioned Parquet Bronze layer
- Azure Data Lake Storage Gen2
- PySpark transformation pipelines
- Delta Lake `MERGE` / upsert processing
- Deduplication using latest-record logic
- Data-quality validation and quarantine
- Slowly Changing Dimension Type 2 for customer history
- Gold star schema for analytics
- Unity Catalog governance
- Databricks Jobs orchestration
- Pipeline audit logging and run metadata
- Terraform infrastructure as code
- GitHub Actions continuous integration
- Automated PySpark tests
- Power BI analytics layer

## Data Model

### Dimensions

- `dim_customer` — SCD Type 2
- `dim_product`
- `dim_date`

### Facts

- `fact_sales`
- `fact_returns`

## Pipeline Flow

1. Synthetic retail transactions are generated in PostgreSQL.
2. Bronze ingestion extracts only new or changed records using `updated_at` watermarks.
3. Bronze data is written as partitioned Parquet locally and to ADLS Gen2.
4. Azure Databricks reads newly ingested Bronze records.
5. Records are deduplicated and validated.
6. Invalid records are written to quarantine.
7. Valid records are merged into Silver Delta tables.
8. Gold dimensions and facts are generated from Silver.
9. Customer history is preserved using SCD Type 2.
10. Gold quality checks prevent invalid analytical data from being published.
11. Unity Catalog exposes governed Gold tables for downstream analytics.
12. Power BI consumes the Gold model.

## Technology Stack

| Area | Technology |
|---|---|
| Language | Python, SQL |
| Source system | PostgreSQL |
| Local runtime | Docker |
| Processing | PySpark |
| Lake format | Parquet, Delta Lake |
| Cloud storage | Azure Data Lake Storage Gen2 |
| Cloud compute | Azure Databricks |
| Governance | Unity Catalog |
| Orchestration | Databricks Jobs |
| Infrastructure | Terraform |
| CI | GitHub Actions |
| BI | Power BI |

## Data Quality

The platform validates:

- missing dimensional keys
- duplicate facts
- negative sales values
- negative refunds
- invalid SCD date ranges
- multiple current customer versions
- malformed or invalid source records

Invalid Silver records are quarantined rather than silently discarded.

## Incremental Processing

RetailPulse uses watermarks to track the most recent successfully processed source timestamp.

A watermark is advanced only after:

1. extraction succeeds,
2. the Bronze file is written,
3. cloud upload succeeds.

This prevents skipped source records after a partial failure.

## Slowly Changing Dimension Type 2

`dim_customer` preserves customer history.

Tracked customer attributes include fields such as:

- name
- email
- address
- location
- customer segment

When a tracked attribute changes:

- the previous record is expired,
- a new version is created,
- historical facts continue to resolve to the correct customer version.

## Audit Logging

Pipeline execution metadata is recorded in PostgreSQL.

Typical audit information includes:

- pipeline name
- processing layer
- source and target
- start and end timestamps
- duration
- rows read
- rows written
- rows quarantined
- start and end watermarks
- status
- error message

## CI/CD

GitHub Actions automatically:

- creates a clean Ubuntu runner
- installs Python
- installs Java
- installs dependencies
- runs PySpark unit tests
- reports pass/fail status on code changes

## Infrastructure as Code

Terraform manages the project infrastructure, including:

- Azure resource group
- ADLS Gen2 storage account
- Databricks access connector
- Unity Catalog storage credential
- external location
- RetailPulse catalog
- Bronze, Silver, Gold, and Quarantine schemas
- Databricks workflow

Imported resources were aligned carefully with Terraform state before applying changes to avoid destructive replacement.

## Local Development

The project can run locally using:

- Docker PostgreSQL
- Docker Spark
- local Parquet Bronze storage
- local Delta Silver and Gold layers

This provides a reproducible development environment before cloud deployment.

## Repository Structure

```text
retailpulse-data-platform/
├── README.md
├── compose.yaml
├── data-generator/
├── ingestion/
├── pipeline_utils/
├── orchestration/
├── databricks/
│   ├── silver_delta/
│   └── gold/
├── tests/
├── terraform/
├── sql/
├── docs/
│   ├── architecture.md
│   ├── data-model.md
│   ├── pipelines.md
│   ├── data-quality.md
│   ├── infrastructure.md
│   └── screenshots/
└── .github/
    └── workflows/
```

## Project Status

| Component | Status |
|---|---|
| Local PostgreSQL source | ✅ |
| Synthetic data generation | ✅ |
| Incremental Bronze ingestion | ✅ |
| ADLS Gen2 | ✅ |
| Azure Databricks | ✅ |
| Silver Delta processing | ✅ |
| Quarantine | ✅ |
| Gold star schema | ✅ |
| SCD Type 2 | ✅ |
| Unity Catalog | ✅ |
| Databricks workflow | ✅ |
| Pipeline audit logging | ✅ |
| PySpark tests | ✅ |
| GitHub Actions CI | ✅ |
| Terraform | ✅ |
| Power BI | In progress |
| Microsoft Fabric | Optional future enhancement |

## Why This Project Matters

RetailPulse demonstrates the full lifecycle of a production-style batch data platform:

- source-system ingestion
- incremental loading
- distributed transformation
- Delta Lake upserts
- dimensional modelling
- historical tracking
- data quality
- governance
- orchestration
- infrastructure as code
- CI
- business intelligence

The project is designed to show both engineering depth and business-facing analytics readiness.

## Project Screenshots

### 1. Azure Resources

Azure resources created for the RetailPulse platform.

![Azure Resources](docs/screenshots/01-azure-resources.png)

---

### 2. Azure Data Lake Storage Gen2

ADLS Gen2 container structure used for Bronze, Silver, Gold, Quarantine, and checkpoint data.

![ADLS Layout](docs/screenshots/02-adls-layout1.png)

![ADLS Bronze Layer](docs/screenshots/02-adls-layout2.png)

![ADLS Silver Layer](docs/screenshots/02-adls-layout3.png)

![ADLS Gold Layer](docs/screenshots/02-adls-layout4.png)

![ADLS Quarantine Layer](docs/screenshots/02-adls-layout5.png)

![ADLS Data Files](docs/screenshots/02-adls-layout6.png)

---

### 3. Unity Catalog

RetailPulse catalog and governed schemas registered in Databricks Unity Catalog.

![Unity Catalog](docs/screenshots/03-unity-catalog.png)

---

### 4. Gold Layer Tables

Analytics-ready Gold tables registered in Unity Catalog.

![Gold Tables](docs/screenshots/04-gold-tables.png)

---

### 5. Databricks Workflow

Databricks Jobs orchestrate the Silver incremental pipeline followed by Gold dimensional modelling.

![Databricks Job](docs/screenshots/05-databricks-job.png)

#### Gold Dimensional Task

![Gold Dimensional Job](docs/screenshots/05-databricks-job-Gold.png)

#### Pipeline Execution

![RetailPulse Cloud Pipeline](docs/screenshots/05-databricks-job-Pipeline.png)

#### Silver Incremental Task

![Silver Incremental Job](docs/screenshots/05-databricks-job-Silver_Incremental.png)

---

### 6. Delta Lake MERGE History

Delta Lake transaction history showing incremental `MERGE` operations.

![Delta Merge History](docs/screenshots/06-delta-merge-history.png)

---

### 7. SCD Type 2

Customer dimension history demonstrating Slowly Changing Dimension Type 2 behaviour.

![SCD Type 2 History](docs/screenshots/07-scd2-example.png)

![SCD Type 2 Example](docs/screenshots/07-scd2-example-2.png)

---

### 8. Terraform Infrastructure

Terraform manages the Azure and Databricks infrastructure as code.

![Terraform Plan](docs/screenshots/08-terraform-plan.png)

---

### 9. GitHub Actions CI

Automated CI pipeline running tests and validating code changes.

![GitHub Actions](docs/screenshots/10-github-actions.png)

---

### 10. Power BI Data Model

Star-schema semantic model built from the RetailPulse Gold layer.

![Power BI Data Model](docs/screenshots/11-powerbi-model.png)

---

### 11. Power BI Dashboard

Business-facing dashboard showing sales, margin, returns, customer segments, and product performance.

![Power BI Dashboard](docs/screenshots/12-powerbi-dashboard.png)
