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
    log_dir: /path/to/logs
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
          write_strategy: upsert
          write_key: [id]

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
  log_dir: ./logs            # Log file directory (optional, logs to console if not set)
  ingested_at_column: _ingested_at  # Column name for ingestion timestamp (default: _ingested_at)
  ingestion_id_column: mkpipe_id   # Column name for dedup hash ID (default: mkpipe_id)
  column_name_case: as_is          # Column name casing: lower, upper, as_is (default: as_is)
  if_exists: replace               # Target table behavior on full load: replace (default) or append

  spark:
    master: "local[*]"       # Spark master URL (default: local[*])
    driver_memory: "4g"      # default: auto-detected from system
    executor_memory: "4g"    # default: auto-detected from system
    extra_config:            # any additional Spark config
      spark.sql.shuffle.partitions: "200"
      spark.dynamicAllocation.enabled: "true"

  backend:
    variant: sqlite          # sqlite (default), postgres, duckdb, clickhouse
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
| `variant` | **Required.** Plugin type: `postgres`, `mysql`, `mongodb`, `file`, etc. |
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
        write_strategy: upsert
        write_key: [id]
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
| `partitions_column_type` | auto | Type of partition column: `int` or `datetime`. Defaults to `int` if `partitions_column` is specified, otherwise inherits `iterate_column_type` |
| `partitions_count` | `10` | Number of JDBC read partitions |
| `fetchsize` | `100000` | JDBC fetch size (rows per network round trip) |
| `batchsize` | `10000` | JDBC write batch size |
| `write_partitions` | `None` | Number of write partitions (coalesce before writing) |
| `dedup_columns` | `None` | Columns for dedup hash generation (xxhash64). Column name configurable via `settings.ingestion_id_column` |
| `custom_query` | `None` | Custom SQL query with `{query_filter}` placeholder |
| `custom_query_file` | `None` | Path to `.sql` file (relative to config directory) |
| `transform` | `None` | Transform function reference: `path/to/file.py::function` |
| `write_strategy` | `None` | Write strategy: `append`, `replace`, `upsert`, `merge` (see below) |
| `write_key` | `None` | Key columns for `upsert`/`merge` (required when strategy is upsert or merge) |
| `if_exists` | `None` | Target table behavior on full load: `replace` (drop+create) or `append` (insert into existing). Inherits from settings if not set |
| `column_name_case` | `None` | Override column casing for this table: `lower`, `upper`, `as_is` (default: inherits from settings) |
| `pass_on_error` | `false` | Continue pipeline on this table's failure |

---

## Write Strategy

`write_strategy` controls how data is written to the destination. If not set, it is inferred automatically: `overwrite` → `replace`, `append` → `append`.

| Strategy | Behavior |
|---|---|
| `append` | Insert new rows. No deduplication. |
| `replace` | Drop/overwrite all existing data, then insert. |
| `upsert` | Insert new rows, update existing rows by `write_key`. Uses `MERGE`/`ON CONFLICT` for SQL databases. |
| `merge` | Full MERGE with matched update + not-matched insert (JDBC loaders only, same as upsert for most targets). |

### Usage

```yaml
tables:
  # Upsert: update existing rows by primary key, insert new ones
  - name: public.users
    target_name: stg_users
    replication_method: incremental
    iterate_column: updated_at
    write_strategy: upsert
    write_key: [id]

  # Replace: full overwrite every run
  - name: public.orders
    target_name: stg_orders
    replication_method: full
    write_strategy: replace

  # Append (default for incremental): just insert
  - name: public.events
    target_name: stg_events
    replication_method: incremental
    iterate_column: created_at
    write_strategy: append
```

### Supported Strategies per Loader

| Loader | append | replace | upsert | merge |
|---|---|---|---|---|
| PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, Redshift, SQLite, TimescaleDB | Y | Y | Y | Y |
| Snowflake | Y | Y | Y | Y |
| BigQuery | Y | Y | Y | Y |
| MongoDB | Y | Y | Y | — |
| ClickHouse | Y | Y | Y | — |
| Elasticsearch | Y | Y | Y | — |
| DynamoDB | Y | Y | Y | — |
| Cassandra | Y | Y | Y | — |
| InfluxDB | Y | Y | Y | — |
| Redis | — | Y | Y | — |
| File (Parquet/CSV/Iceberg/Delta) | Y | Y | — | — |

### Validation Rules

- `write_strategy: upsert` or `merge` **requires** `write_key` — raises `ConfigError` if missing.
- `write_key` is ignored when strategy is `append` or `replace`.
- If `write_strategy` is not set, the strategy is inferred from the extractor's write mode.

---

## If Exists (Target Table Preservation)

`if_exists` controls whether the target table is dropped and recreated or preserved on full load (`replace` strategy).

| Value | Behavior |
|---|---|
| `replace` | Drop existing target table and recreate it (default) |
| `append` | Preserve existing target table structure, insert data without dropping |

This is useful when you want to keep the target table schema (e.g. indexes, constraints, grants) intact across runs.

### Global Setting

```yaml
settings:
  if_exists: replace    # default: drop and recreate target tables
```

### Per-Table Override

```yaml
tables:
  - name: public.users
    target_name: stg_users
    if_exists: append     # preserve this table, just insert

  - name: public.orders
    target_name: stg_orders
    # inherits settings.if_exists (replace)
```

Table-level `if_exists` takes priority over the settings-level default. If the table does not specify it, the settings value is used.

> **Note:** `if_exists` only affects the `replace` write strategy (full load). It has no effect on `append`, `upsert`, or `merge` strategies, which never drop the target table.

---

## Column Name Casing

Some databases (Snowflake, Oracle) return uppercase column names by default. When writing to a case-sensitive target like PostgreSQL, this results in uppercase quoted identifiers that are inconvenient to query.

Use `column_name_case` to normalize column names before writing:

```yaml
settings:
  column_name_case: lower    # lower, upper, or as_is (default: as_is)
```

This can also be overridden per table:

```yaml
tables:
  - name: SNOWFLAKE_SCHEMA.USERS
    target_name: stg_users
    column_name_case: lower   # override for this table only
```

| Value | Behavior |
|-------|----------|
| `as_is` | No transformation (default) |
| `lower` | All column names converted to lowercase |
| `upper` | All column names converted to uppercase |

Table-level `column_name_case` takes priority over the settings-level default. If the table does not specify it, the settings value is used.

---

## Incremental Replication

mkpipe uses an **append-only** strategy for incremental replication:

1. **Extract**: reads rows where `iterate_column >= last_point` (inclusive, no boundary loss)
2. **Load**: appends to the target table (never overwrites or deletes)
3. **Dedup**: if `dedup_columns` is set, a `mkpipe_id` (xxhash64 hash) is generated for downstream deduplication

An ingestion timestamp column is always added to every row. The column name defaults to `_ingested_at` and can be customized via `settings.ingested_at_column`.

```yaml
- name: public.users
  target_name: stg_users
  replication_method: incremental
  iterate_column: updated_at
  iterate_column_type: datetime
  dedup_columns: [id, updated_at]  # mkpipe_id = xxhash64(id, updated_at)
```

### NULL Handling in iterate_column

On **initial load** (no previous checkpoint), the extractor automatically includes rows where `iterate_column` evaluates to `NULL`. This prevents data loss when using expressions like `greatest(cdate, udate)` where both source columns can be `NULL`.

- **Initial load**: `WHERE (col >= min AND col <= max) OR col IS NULL`
- **Incremental load**: `WHERE col >= last_point AND col <= max` (NULL rows already captured)

If **all** rows have a `NULL` iterate_column, the extractor falls back to a full extraction.

### Partition Column Type Behavior

The `partitions_column_type` parameter controls how partition bounds are converted for Spark JDBC partitioning:

**Scenario 1: No partition column specified (default)**
```yaml
- name: public.orders
  replication_method: incremental
  iterate_column: created_at
  iterate_column_type: datetime
  # partitions_column defaults to created_at
  # partitions_column_type inherits datetime from iterate_column_type
```

**Scenario 2: Integer partition column specified**
```yaml
- name: public.customers
  replication_method: incremental
  iterate_column: updated_at
  iterate_column_type: datetime
  partitions_column: customer_id
  # partitions_column_type defaults to 'int' (most partition keys are integers)
  # Handles PostgreSQL NUMERIC/DECIMAL types correctly
```

**Scenario 3: Explicit datetime partition column**
```yaml
- name: public.events
  replication_method: incremental
  iterate_column: event_id
  iterate_column_type: int
  partitions_column: event_timestamp
  partitions_column_type: datetime  # Must be explicit if different from iterate_column_type
```

Downstream dedup query example:

```sql
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY mkpipe_id ORDER BY _ingested_at DESC) AS rn
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