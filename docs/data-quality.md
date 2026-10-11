# RetailPulse Data Quality

## Purpose

RetailPulse treats data quality as part of the pipeline rather than a separate reporting exercise.

Quality checks are applied at both Silver and Gold layers.

## Silver Validation

Silver validates incoming source records before merging them into curated Delta tables.

Examples include:

- required identifiers
- valid numeric values
- valid calculated values
- normalized fields
- source-rule consistency

Invalid rows are written to quarantine.

## Quarantine Pattern

Instead of dropping bad records:

```text
Incoming Bronze
      ↓
Validation
   ↙      ↘
Valid    Invalid
  ↓         ↓
Silver   Quarantine
```

Benefits:

- no silent data loss
- easier troubleshooting
- auditability
- reproducibility
- clear separation between accepted and rejected data

## Gold Quality Gates

Gold quality checks run after dimensional processing.

Checks include:

- missing customer surrogate keys
- missing product surrogate keys
- duplicate fact rows
- negative sales values
- negative refund values
- multiple current SCD customer versions
- invalid `valid_from` / `valid_to` ranges

If a quality gate fails, the Gold build raises an error rather than publishing a silently incorrect result.

## SCD Type 2 Integrity

Important SCD checks include:

### One current version per customer

A customer should not have multiple rows where:

```text
is_current = true
```

### Valid date ranges

Historical versions must satisfy:

```text
valid_from < valid_to
```

for closed records.

### Historical fact resolution

Facts must resolve to a valid historical customer version based on event date.

## Testing

Automated PySpark tests cover transformation behavior such as:

- email normalization
- invalid customer quarantine
- order-item calculations
- invalid order-item quarantine
- latest-record deduplication

## Observability

Pipeline metadata records:

- rows read
- rows written
- rows quarantined
- status
- duration
- watermark movement
- errors

This makes quality issues visible at run level as well as row level.

## Design Principle

The platform prefers:

```text
detect → isolate → record → fail visibly
```

over:

```text
ignore → continue silently
```
