# RetailPulse Infrastructure

## Overview

RetailPulse uses Terraform to manage Azure and Databricks infrastructure.

## Azure Resources

### Resource Group

```text
RetailPulse
```

### ADLS Gen2 Storage Account

```text
stretailpulsedev21
```

Characteristics:

- StorageV2
- hierarchical namespace enabled
- Standard performance
- LRS replication

Container:

```text
retailpulse
```

Main paths:

```text
bronze/
silver/
gold/
quarantine/
checkpoints/
```

### Azure Databricks

Workspace:

```text
dbw-retailpulse-dev
```

The Databricks workspace is referenced as a Terraform data source rather than managed as a Terraform-created resource.

### Databricks Access Connector

```text
ac-retailpulse-dev
```

The access connector uses a system-assigned managed identity.

It is granted access to ADLS and is used by Unity Catalog storage credentials.

## Unity Catalog

Catalog:

```text
retailpulse
```

Schemas:

```text
bronze
silver
gold
quarantine
```

Storage credential:

```text
retailpulse_storage_credential
```

External location:

```text
retailpulse_adls
```

The external location points to the RetailPulse ADLS container.

## Terraform-Managed Databricks Objects

Terraform manages:

- storage credential
- external location
- catalog
- schemas
- Databricks workflow

The workflow is:

```text
RetailPulse Cloud Pipeline
```

with tasks:

```text
Silver_Incremental
Gold_Dimensional
```

## Import Strategy

Several resources existed before Terraform management.

They were imported into Terraform state and then aligned with configuration.

This avoided rebuilding live infrastructure.

A key safety rule during migration was:

```text
Do not apply plans that propose unintended destroy/replacement.
```

Terraform plans were adjusted until imported resources could be updated in place.

## Managed-Identity Access

The data-access pattern is:

```text
Databricks
   ↓
Storage Credential
   ↓
Azure Databricks Access Connector
   ↓
Managed Identity
   ↓
ADLS Gen2
```

This avoids embedding cloud storage secrets in Databricks notebooks.

## Terraform Safety Controls

For imported Databricks resources, lifecycle handling is used where provider-generated properties should not be managed directly.

Examples include Databricks-generated:

- catalog storage metadata
- schema properties
- file-event queue internals

This keeps Terraform focused on intentional configuration.

## Local Secrets

Local development uses environment variables for PostgreSQL and Azure configuration.

The `.env` file is excluded from Git.

## Git Hygiene

Terraform transient and sensitive state is excluded from version control.

Examples:

```gitignore
.terraform/
**/.terraform/
*.tfstate
*.tfstate.*
```

The provider lock file is committed:

```text
terraform/.terraform.lock.hcl
```

This improves reproducibility while keeping provider binaries and state out of Git.

## CI

GitHub Actions runs automated validation and tests on code changes.

A future enhancement can include:

```text
terraform fmt -check
terraform validate
```

as part of CI.
