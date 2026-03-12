# mkpipe

**mkpipe** is a Spark-based, modular ETL framework. It provides a Python-first API for building data pipelines with pluggable extractors, loaders, and inline transformations.

## Key Features

- **PySpark engine** — parallel, partitioned JDBC reads/writes for high-throughput data movement
- **Modular plugins** — `pip install mkpipe-extractor-postgres` to add a source, `pip install mkpipe-loader-postgres` to add a destination
- **Single YAML config** — define connections, pipelines, and tables in one file
- **Tag-based execution** — assign tags to tables and run only what you need across all pipelines
- **Orchestrator-agnostic** — use with Dagster, Airflow, cron, or any Python scheduler
- **Inline transformations** — reference a Python function (`df → df`) directly in YAML
- **Dependency Injection** — pass your own SparkSession (Glue, EMR, Dataproc) or let mkpipe create one
- **Incremental & full replication** — append-only incremental with `mkpipe_id` for idempotent deduplication

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
      variant: postgres
      host: localhost
      port: 5432
      database: source_db
      user: ${PG_USER}
      password: ${PG_PASSWORD}
      schema: public

    target_pg:
      variant: postgres
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
          tags: [api, user-domain]
          replication_method: incremental
          iterate_column: updated_at
          iterate_column_type: datetime
          dedup_columns: [id, updated_at]

        - name: public.orders
          target_name: stg_orders
          tags: [api, order-domain]
          replication_method: full
```

Run it:

```bash
# Run all tables
mkpipe run

# Run only tables tagged "api"
mkpipe run --tags api
```

---

## Python API

```python
import mkpipe

# Run all pipelines
mkpipe.run(config="mkpipe_project.yaml")

# Run a specific pipeline
mkpipe.run(config="mkpipe_project.yaml", pipeline="my_pipeline")

# Run a specific table
mkpipe.run(config="mkpipe_project.yaml", table="stg_users")

# Run by tags — runs matching tables across ALL pipelines
mkpipe.run(config="mkpipe_project.yaml", tags=["api"])
mkpipe.run(config="mkpipe_project.yaml", tags=["api", "order-domain"])

# Combine filters: pipeline + tags
mkpipe.run(config="mkpipe_project.yaml", pipeline="my_pipeline", tags=["api"])

# Pass a custom SparkSession (e.g. AWS Glue, EMR, Dataproc)
mkpipe.run(config="mkpipe_project.yaml", spark=my_spark_session)

# Extract only — returns ExtractResult with a Spark DataFrame
result = mkpipe.extract(config="mkpipe_project.yaml", table="stg_users")
df = result.df

# Load only — pass your own DataFrame
mkpipe.load(config="mkpipe_project.yaml", table="stg_users", df=my_df)
```

---

## CLI Reference

```bash
mkpipe run [OPTIONS]
mkpipe install-jars
```

| Command / Option | Short | Description |
|---|---|---|
| `mkpipe run` | | Run pipelines from config file |
| `--config` | `-c` | Path to config file. Default: `mkpipe_project.yaml` in current dir |
| `--pipeline` | `-p` | Run only the named pipeline |
| `--table` | `-t` | Run only the named table (source name or target name) |
| `--tags` | | Comma-separated tags to filter tables, e.g. `--tags api,ingestion` |
| `mkpipe install-jars` | | Download Maven JARs for all installed plugins (offline/Docker use) |

Examples:

```bash
# Run everything
mkpipe run

# Specific config file
mkpipe run --config /path/to/config.yaml

# Single pipeline
mkpipe run -p my_pipeline

# Single table
mkpipe run -t stg_users

# By tags (OR logic: any matching tag)
mkpipe run --tags api
mkpipe run --tags api,ingestion

# Combine: pipeline + tags
mkpipe run -p my_pipeline --tags api
```

---

## Tags

Tags let you group tables by business domain, team, priority, or any criteria. When you pass `tags`, mkpipe runs **all matching tables across all pipelines** (OR logic).

```yaml
pipelines:
  - name: pg_to_pg
    source: source_pg
    destination: target_pg
    tables:
      - name: public.users
        target_name: stg_users
        tags: [api, user-domain, critical]

      - name: public.sessions
        target_name: stg_sessions
        tags: [api, user-domain]

  - name: mysql_to_pg
    source: source_mysql
    destination: target_pg
    tables:
      - name: orders
        target_name: stg_orders
        tags: [api, order-domain, critical]
```

```python
# Runs stg_users + stg_sessions + stg_orders (all have "api")
mkpipe.run(config="config.yaml", tags=["api"])

# Runs stg_users + stg_orders (both have "critical")
mkpipe.run(config="config.yaml", tags=["critical"])

# Runs stg_users + stg_sessions (both have "user-domain")
mkpipe.run(config="config.yaml", tags=["user-domain"])

# OR logic: runs anything tagged "critical" OR "order-domain"
mkpipe.run(config="config.yaml", tags=["critical", "order-domain"])
```

---

## YAML Configuration Reference

### Top-level Structure

```yaml
version: 2
default_environment: prod    # which environment block to use

prod:                        # environment name
  settings: ...
  connections: ...
  pipelines: ...

staging:                     # you can define multiple environments
  settings: ...
  connections: ...
  pipelines: ...
```

### Settings

```yaml
settings:
  timezone: UTC              # Spark session timezone (default: UTC)

  spark:
    master: "local[*]"       # Spark master URL (default: local[*])
    driver_memory: "4g"      # default: auto-detected from system
    executor_memory: "4g"    # default: auto-detected from system
    extra_config:            # any additional Spark config
      spark.sql.shuffle.partitions: "200"
      spark.dynamicAllocation.enabled: "true"

  backend:
    variant: sqlite          # sqlite (default), postgresql, duckdb, clickhouse
    host: localhost
    port: 5432
    database: mkpipe_db
    user: mkpipe
    password: ${BACKEND_PASSWORD}
```

### Connections

```yaml
connections:
  my_postgres:
    variant: postgres
    host: ${PG_HOST}
    port: 5432
    database: mydb
    user: ${PG_USER}
    password: ${PG_PASSWORD}
    schema: public

  my_mongodb:
    variant: mongodb
    mongo_uri: ${MONGO_URI}
    database: mydb

  my_s3:
    variant: file
    extra:
      storage: s3
      format: parquet
      path: s3a://my-bucket/data
    aws_access_key: ${AWS_ACCESS_KEY}
    aws_secret_key: ${AWS_SECRET_KEY}
    region: eu-west-1
```

Environment variables are referenced with `${VAR_NAME}` syntax and resolved at load time.

### Connection Parameters

| Parameter | Description |
|---|---|
| `variant` | **Required.** Plugin type: `postgresql`, `mysql`, `mongodb`, `file`, etc. |
| `host` | Database host |
| `port` | Database port |
| `database` | Database name |
| `user` | Username |
| `password` | Password |
| `schema` | Schema name |
| `warehouse` | Warehouse (Snowflake) |
| `private_key_file` | Path to private key file (RSA auth) |
| `private_key_file_pwd` | Private key passphrase |
| `mongo_uri` | Full MongoDB connection URI |
| `bucket_name` | S3/GCS bucket name |
| `s3_prefix` | S3 key prefix |
| `aws_access_key` | AWS access key |
| `aws_secret_key` | AWS secret key |
| `region` | Cloud region |
| `credentials_file` | Path to credentials file (GCS service account) |
| `api_key` | API key |
| `oauth_token` | OAuth token |
| `client_id` | OAuth client ID |
| `client_secret` | OAuth client secret |
| `extra` | Dict of additional options (storage, format, path, etc.) |

### Pipelines & Tables

```yaml
pipelines:
  - name: my_pipeline        # unique pipeline name
    source: source_pg         # connection name for extraction
    destination: target_pg    # connection name for loading
    pass_on_error: false      # if true, continue on table failure
    tables:
      - name: public.users
        target_name: stg_users
        tags: [api, user-domain]
        replication_method: incremental
        iterate_column: updated_at
        iterate_column_type: datetime
        partitions_column: id
        partitions_count: 10
        fetchsize: 100000
        batchsize: 10000
        write_partitions: 4
        dedup_columns: [id, updated_at]
        custom_query: "(SELECT id, name, updated_at FROM users {query_filter}) q"
        transform: transforms/clean_users.py::transform
        pass_on_error: false
```

### Table Parameters

| Parameter | Default | Description |
|---|---|---|
| `name` | **required** | Source table/collection name |
| `target_name` | **required** | Destination table name |
| `tags` | `[]` | List of tags for filtering (`--tags api,ingestion`) |
| `replication_method` | `full` | `full` or `incremental` |
| `iterate_column` | `None` | Column for incremental tracking (required if incremental) |
| `iterate_column_type` | `None` | `datetime` or `int` |
| `partitions_column` | iterate_column | Column for Spark JDBC partitioning |
| `partitions_count` | `10` | Number of JDBC read partitions |
| `fetchsize` | `100000` | JDBC fetch size (rows per network round trip) |
| `batchsize` | `10000` | JDBC write batch size |
| `write_partitions` | `None` | Number of write partitions (coalesce before writing) |
| `dedup_columns` | `None` | Columns for `mkpipe_id` hash generation (xxhash64) |
| `custom_query` | `None` | Custom SQL query with `{query_filter}` placeholder |
| `custom_query_file` | `None` | Path to `.sql` file (relative to `sql/` directory) |
| `transform` | `None` | Transform function reference: `path/to/file.py::function` |
| `pass_on_error` | `false` | Continue pipeline on this table's failure |

---

## Incremental Replication

mkpipe uses an **append-only** strategy for incremental replication:

1. **Extract**: reads rows where `iterate_column >= last_point` (inclusive, no boundary loss)
2. **Load**: appends to the target table (never overwrites or deletes)
3. **Dedup**: if `dedup_columns` is set, a `mkpipe_id` (xxhash64 hash) is generated for downstream deduplication

An `etl_time` timestamp is always added to every row.

```yaml
- name: public.users
  target_name: stg_users
  replication_method: incremental
  iterate_column: updated_at
  iterate_column_type: datetime
  dedup_columns: [id, updated_at]  # mkpipe_id = xxhash64(id, updated_at)
```

Downstream dedup query example:

```sql
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY mkpipe_id ORDER BY etl_time DESC) AS rn
  FROM stg_users
) WHERE rn = 1
```

---

## Inline Transformations

Add a `transform` field to any table:

```yaml
tables:
  - name: public.products
    target_name: stg_products
    replication_method: full
    transform: transforms/clean_products.py::transform
```

The transform function receives and returns a PySpark DataFrame:

```python
# transforms/clean_products.py
def transform(df):
    df = df.filter(df.status != "deleted")
    return df
```

---

## Orchestrator Integration

### Dagster

```python
from dagster import asset, Definitions
import mkpipe

@asset
def api_tables():
    mkpipe.run(config="mkpipe_project.yaml", tags=["api"])

@asset
def critical_tables():
    mkpipe.run(config="mkpipe_project.yaml", tags=["critical"])

defs = Definitions(assets=[api_tables, critical_tables])
```

### Airflow

```python
from airflow.decorators import task

@task
def sync_api_tables():
    import mkpipe
    mkpipe.run(config="/path/to/mkpipe_project.yaml", tags=["api"])

@task
def sync_user_domain():
    import mkpipe
    mkpipe.run(config="/path/to/mkpipe_project.yaml", tags=["user-domain"])
```

---

## Backend (State Tracking)

mkpipe tracks pipeline state (last sync point, status) in a manifest database. Default is SQLite (zero-config). PostgreSQL, DuckDB, and ClickHouse are also supported:

```yaml
settings:
  backend:
    variant: postgres
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

---

## Custom Exceptions

mkpipe provides specific exception classes for clean error handling:

```python
from mkpipe import (
    MkpipeError,          # base exception
    ConfigError,          # YAML or configuration issues
    ExtractionError,      # data extraction failures
    LoadError,            # data loading failures
    TransformError,       # transformation failures
    PluginNotFoundError,  # missing plugin
    BackendError,         # backend manifest failures
)
```

---

## JAR Management

mkpipe plugins that depend on JDBC drivers or Spark connectors need JAR files. mkpipe handles this **automatically** — no manual steps required for most users.

### Online (Default) — Lazy Download

When mkpipe starts, it detects which plugins are installed and resolves their Maven dependencies via `spark.jars.packages`. JARs are downloaded on first run and cached by Spark's Ivy resolver.

```
# Nothing to do — just run your pipeline
mkpipe run
```

### Offline / Docker — Pre-download JARs

For **air-gapped** or **on-premise** environments without internet access, pre-download all JARs during the Docker build:

```bash
mkpipe install-jars
```

This command:
1. Discovers all installed plugins and their Maven dependencies
2. Downloads JARs via Spark's Ivy resolver into a temp directory
3. Copies them into each plugin's `jars/` directory
4. Cleans up the temp Ivy cache

**Dockerfile example:**

```dockerfile
FROM python:3.11-slim

# Install Java (required for PySpark)
RUN apt-get update && apt-get install -y default-jdk && rm -rf /var/lib/apt/lists/*

# Install mkpipe and plugins
RUN pip install mkpipe mkpipe-extractor-postgres mkpipe-loader-clickhouse

# Pre-download JARs (no internet needed at runtime)
RUN mkpipe install-jars

COPY mkpipe_project.yaml .
CMD ["mkpipe", "run"]
```

### How It Works

| Scenario | Local JARs in `jars/` | Maven resolution |
|---|---|---|
| Fresh install, online | No | Yes — `spark.jars.packages` |
| After `mkpipe install-jars` | Yes | No — local JARs used |
| Plugin with custom JAR (e.g. MongoDB `mkpipe-tls-helper.jar`) | Yes (custom) | Yes — only for missing Maven deps |

### CLI Reference

```bash
mkpipe install-jars    # Download all Maven JARs for installed plugins
```

---

## Available Plugins

**For the full list, visit the [mkpipe-hub](https://github.com/mkpipe-etl/mkpipe-hub).**

### Extractors
- PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, SQLite, Redshift, ClickHouse, MongoDB, Snowflake, BigQuery, Cassandra, TimescaleDB, DynamoDB, Elasticsearch, InfluxDB, Redis, File (S3/GCS/local/Iceberg/Delta)

### Loaders
- PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, SQLite, Redshift, ClickHouse, MongoDB, Snowflake, BigQuery, Cassandra, TimescaleDB, DynamoDB, Elasticsearch, InfluxDB, Redis, File (S3/GCS/local/Iceberg/Delta)

---

## License

Apache 2.0 — see [LICENSE](LICENSE).