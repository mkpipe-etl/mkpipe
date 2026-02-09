# mkpipe

**mkpipe** is a Spark-based, modular ETL framework. It provides a Python-first API for building data pipelines with pluggable extractors, loaders, and inline transformations.

## Key Features

- **PySpark engine** — parallel, partitioned JDBC reads/writes for high-throughput data movement
- **Modular plugins** — `pip install mkpipe-extractor-postgres` to add a source, `pip install mkpipe-loader-postgres` to add a destination
- **Single YAML config** — define connections, pipelines, and tables in one file
- **Orchestrator-agnostic** — use with Dagster, Airflow, cron, or any Python scheduler
- **Inline transformations** — reference a Python function (`df → df`) directly in YAML
- **Dependency Injection** — pass your own SparkSession (Glue, EMR, Dataproc) or let mkpipe create one
- **Incremental & full replication** — built-in support for both strategies with automatic state tracking

## Quick Start

```bash
pip install mkpipe mkpipe-extractor-postgres mkpipe-loader-postgres
```

Create `mkpipe_project.yaml`:

```yaml
version: 2
default_environment: prod

prod:
  settings:
    timezone: UTC
    backend:
      variant: sqlite

  connections:
    source_pg:
      variant: postgresql
      host: localhost
      port: 5432
      database: source_db
      user: ${PG_USER}
      password: ${PG_PASSWORD}
      schema: public

    target_pg:
      variant: postgresql
      host: localhost
      port: 5432
      database: dwh_db
      user: ${PG_USER}
      password: ${PG_PASSWORD}
      schema: staging

  pipelines:
    - name: my_pipeline
      source: source_pg
      destination: target_pg
      tables:
        - name: public.users
          target_name: stg_users
          replication_method: incremental
          iterate_column: updated_at
          iterate_column_type: datetime

        - name: public.orders
          target_name: stg_orders
          replication_method: full
```

Run it:

```bash
mkpipe run --config mkpipe_project.yaml
```

## Python API

```python
import mkpipe

# Run all pipelines
mkpipe.run(config="mkpipe_project.yaml")

# Run a specific pipeline
mkpipe.run(config="mkpipe_project.yaml", pipeline="my_pipeline")

# Run a specific table
mkpipe.run(config="mkpipe_project.yaml", table="stg_users")

# Pass a custom SparkSession (e.g. AWS Glue, EMR)
mkpipe.run(config="mkpipe_project.yaml", spark=my_spark_session)

# Extract only — returns ExtractResult with a Spark DataFrame
result = mkpipe.extract(config="mkpipe_project.yaml", table="stg_users")
df = result.df

# Load only — pass your own DataFrame
mkpipe.load(config="mkpipe_project.yaml", table="stg_users", df=my_df)
```

## Orchestrator Integration

### Dagster

```python
from dagster import asset, Definitions
import mkpipe

@asset
def users():
    mkpipe.run(config="mkpipe_project.yaml", table="stg_users")

@asset
def orders():
    mkpipe.run(config="mkpipe_project.yaml", table="stg_orders")

defs = Definitions(assets=[users, orders])
```

### Airflow

```python
from airflow.decorators import task

@task
def sync_users():
    import mkpipe
    mkpipe.run(config="/path/to/mkpipe_project.yaml", table="stg_users")
```

## Inline Transformations

Add a `transform` field to any table in YAML:

```yaml
tables:
  - name: public.products
    target_name: stg_products
    replication_method: full
    transform: transforms/clean_products.py::transform
```

The transform function receives and returns a Spark DataFrame:

```python
# transforms/clean_products.py
def transform(df):
    df = df.filter(df.status != "deleted")
    return df
```

## Backend (State Tracking)

mkpipe tracks pipeline state (last sync point, status) in a manifest database. Default is SQLite (zero-config). PostgreSQL, DuckDB, and ClickHouse are also supported:

```yaml
settings:
  backend:
    variant: postgresql
    host: localhost
    port: 5432
    database: mkpipe_db
    user: mkpipe
    password: ${BACKEND_PASSWORD}
```

Install optional backend dependencies:

```bash
pip install mkpipe[postgres-backend]
pip install mkpipe[duckdb-backend]
pip install mkpipe[clickhouse-backend]
pip install mkpipe[all-backends]
```

## Available Plugins

**For the full list, visit the [mkpipe-hub](https://github.com/mkpipe-etl/mkpipe-hub).**

### Extractors
- PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, SQLite, Redshift, ClickHouse, S3, Snowflake

### Loaders
- PostgreSQL, MySQL, MariaDB, SQLite, S3, Snowflake, ClickHouse

## License

Apache 2.0 — see [LICENSE](LICENSE).