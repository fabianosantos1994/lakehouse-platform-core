# Lakehouse Platform Core - Ingestion Engine V1

## Objective

This repository contains the reusable execution core for the local Lakehouse Ingestion Engine V1.

The first validated V1 flow supports:

- relational source ingestion from Postgres
- Bronze as raw Parquet files in object storage
- Silver as technical Iceberg tables
- one-table-at-a-time execution through CLI

## Current V1 Scope

### Supported source

- Postgres only

### Supported load type

- full load only

### Supported layers

- Bronze
- Silver

### Layer semantics

#### Bronze

- raw Parquet files
- stored in object storage
- derived physical path controlled by the engine

#### Silver

- technical Iceberg tables
- registered in catalog `lakehouse`
- physical location explicitly controlled by the engine

## Validated Flow

```text
Postgres -> Bronze raw parquet -> Silver Iceberg
```

## Repository Responsibility

`lakehouse-platform-core` contains reusable execution logic for the ingestion engine.

It is responsible for:

- loading ingestion YAML definitions
- validating the V1 YAML contract
- deriving physical Bronze and Silver paths
- resolving Postgres connection settings from environment variables
- reading Postgres tables through Spark JDBC
- writing Bronze raw Parquet files
- writing Silver Iceberg tables
- executing one table and one layer at a time

This repository does not contain pipeline definitions. YAML ingestion definitions live in `lakehouse-ingestion-engine`.

## Example YAML

```yaml
job:
  name: if_purchase
  schedule: "0 2 * * *"

source:
  type: relational
  database_type: postgres
  database_name: purchase
  connection_id: purchase_postgres

tables:
  - name: transactions
    source:
      table: public.transactions
    target:
      silver_table: lakehouse.silver_purchase.public_transactions
    load:
      type: full

  - name: customers
    source:
      table: public.customers
    target:
      silver_table: lakehouse.silver_purchase.public_customers
    load:
      type: full
```

## Naming and Path Conventions

For a source table:

```text
public.transactions
```

The engine derives:

```text
schema_table_name = public_transactions
```

For V1:

```text
engine_source = postgres
engine_extract = spark
source_database = purchase
```

Bronze path:

```text
s3a://lakehouse/bronze/postgres/spark/purchase/public_transactions/
```

Silver path:

```text
s3a://lakehouse/silver/postgres/spark/purchase/public_transactions/
```

Silver logical table:

```text
lakehouse.silver_purchase.public_transactions
```

## Required Environment Variables

Spark / MinIO access:

```env
S3_ACCESS_KEY=minio
S3_SECRET_KEY=minio123
```

Postgres source connection for `connection_id: purchase_postgres`:

```env
PURCHASE_POSTGRES_HOST=postgres
PURCHASE_POSTGRES_PORT=5432
PURCHASE_POSTGRES_DATABASE=purchase
PURCHASE_POSTGRES_USERNAME=<username>
PURCHASE_POSTGRES_PASSWORD=<password>
```

Credentials are not stored in YAML.

## PostgreSQL JDBC Driver Requirement

The Spark runtime must include the PostgreSQL JDBC driver.

Expected driver class:

```text
org.postgresql.Driver
```

The current V1 relational connector uses Spark JDBC with:

```text
format = jdbc
driver = org.postgresql.Driver
```

## CLI Execution Examples

Run Bronze for one table:

```bash
lakehouse-ingestion \
  --config /opt/configs/ingestion/purchase/full.yaml \
  --table transactions \
  --step bronze
```

Run Silver for one table:

```bash
lakehouse-ingestion \
  --config /opt/configs/ingestion/purchase/full.yaml \
  --table transactions \
  --step silver
```

Bronze must be executed before Silver because Silver reads from the derived Bronze raw Parquet path.

## Validation Examples

### Spark SQL

Check the Silver Iceberg table:

```sql
SELECT *
FROM lakehouse.silver_purchase.public_transactions;
```

Count rows:

```sql
SELECT COUNT(*)
FROM lakehouse.silver_purchase.public_transactions;
```

Check table metadata:

```sql
DESCRIBE TABLE lakehouse.silver_purchase.public_transactions;
```

### MinIO

Expected Bronze location:

```text
s3a://lakehouse/bronze/postgres/spark/purchase/public_transactions/
```

Expected Silver location:

```text
s3a://lakehouse/silver/postgres/spark/purchase/public_transactions/
```

Bronze should contain raw Parquet files.

Silver should contain Iceberg table data and metadata under the explicitly derived Silver path.

## Current Limitations

- Only Postgres is supported.
- Only full load is supported.
- Only one table is executed per CLI command.
- Bronze does not add metadata columns yet.
- Silver is technical only and does not apply business rules.
- Incremental ingestion is not implemented.
- File ingestion is not implemented.
- Airflow DAG generation is not implemented.
- No retries, data quality checks, schema evolution, compaction, merge/upsert, or partitioning are implemented yet.
