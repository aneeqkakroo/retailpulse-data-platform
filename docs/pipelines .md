# RetailPulse Pipelines

## Overview

RetailPulse uses a layered batch pipeline:

```text
PostgreSQL
   ↓
Bronze
   ↓
Silver
   ↓
Gold
```

The pipeline supports both local development and Azure execution.

## Source Data Generation

Synthetic source data is generated into PostgreSQL.

Source entities:

- customers
- products
- orders
- order_items
- returns

A daily update process simulates:

- new orders
- order status changes
- customer changes

This allows the pipeline to demonstrate incremental ingestion and SCD Type 2 behavior.

## Bronze Pipeline

The Bronze pipeline extracts changed source records using `updated_at`.

### Watermark Logic

Each table has a stored watermark.

Conceptually:

```sql
SELECT *
FROM source_table
WHERE updated_at > last_successful_watermark;
```

The watermark is updated only after the Bronze write and cloud upload succeed.

This reduces the risk of data loss after partial pipeline failure.

### Bronze Output

Files are written under:

```text
bronze/<table>/load_date=YYYY-MM-DD/
```

Each Bronze record also includes technical metadata.

## Silver Pipeline

The Silver pipeline runs in PySpark / Databricks.

### Processing Steps

1. read newly ingested Bronze records
2. filter using Silver checkpoint
3. deduplicate
4. transform
5. validate
6. route invalid rows to quarantine
7. merge valid rows into Silver Delta
8. save checkpoint

### Deduplication

The latest source version is selected using:

```text
updated_at DESC
_ingested_at DESC
```

within each source primary key.

### Delta MERGE

Silver uses Delta `MERGE` so repeated execution is idempotent.

Conceptually:

```text
MATCHED     → UPDATE
NOT MATCHED → INSERT
```

## Gold Pipeline

The Gold pipeline creates the analytical star schema.

Build steps:

1. build `dim_customer`
2. build `dim_product`
3. build `dim_date`
4. build `fact_sales`
5. build `fact_returns`
6. run quality checks

## Databricks Workflow

The cloud workflow is:

```text
RetailPulse Cloud Pipeline

Silver_Incremental
        ↓
Gold_Dimensional
```

The Gold task depends on successful completion of Silver.

Retries are configured for both tasks.

## Local Orchestration

Local orchestration runs the same conceptual stages:

```text
Bronze Python ingestion
        ↓
Silver Spark container
        ↓
Gold Spark container
```

The orchestration layer is fail-fast and records a parent pipeline run.

## Checkpointing

Silver checkpoints are stored separately from source watermarks.

Source watermark:

```text
What has been safely extracted from PostgreSQL?
```

Silver checkpoint:

```text
What Bronze data has been safely processed into Silver?
```

The separation prevents one layer's state from incorrectly advancing another layer.

## Failure Handling

The pipeline is designed so failed stages do not silently advance state.

Examples:

- failed ADLS upload → Bronze watermark is not advanced
- failed Silver processing → Silver checkpoint is not advanced
- failed Gold quality check → publication fails
