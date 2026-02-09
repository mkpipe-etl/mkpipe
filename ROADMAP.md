# mkpipe Roadmap

## Phase 1 — Core + JDBC Plugins ✅

- Core paket: models, config, backend, spark, transform, plugins, api, cli
- JDBC base: `JdbcExtractor` / `JdbcLoader` with `_jdbc_options()` hook
- Backend: SQLite (default), PostgreSQL, DuckDB, ClickHouse

---

## Plugin List (1–100)

### 0–10: Core Relational Databases + File Storage

| # | DB | Variant | Type | Base Class | Status |
|---|---|---|---|---|---|
| 0 | PostgreSQL | `postgresql` | JDBC | JdbcExtractor/JdbcLoader | ✅ |
| 1 | MySQL | `mysql` | JDBC | JdbcExtractor/JdbcLoader | ✅ |
| 2 | MariaDB | `mariadb` | JDBC | JdbcExtractor/JdbcLoader | ✅ |
| 3 | SQL Server | `sqlserver` | JDBC | JdbcExtractor/JdbcLoader | ✅ |
| 4 | Oracle | `oracledb` | JDBC | JdbcExtractor/JdbcLoader | ✅ |
| 5 | SQLite | `sqlite` | JDBC | JdbcExtractor/JdbcLoader | ✅ |
| 6 | Snowflake | `snowflake` | JDBC + RSA | JdbcExtractor/JdbcLoader | ✅ |
| 7 | Google BigQuery | `bigquery` | Spark native | BaseExtractor/BaseLoader | ✅ |
| 8 | Amazon Redshift | `redshift` | JDBC | JdbcExtractor/JdbcLoader | ✅ |
| 9 | ClickHouse | `clickhouse` | JDBC | JdbcExtractor/JdbcLoader | ✅ |
| 10 | File (S3/ADLS/GCS/HDFS/local) | `file` | Spark native | BaseExtractor/BaseLoader | ✅ |

### 11–20: NoSQL Databases

| # | DB | Variant | Type | Base Class | Status |
|---|---|---|---|---|---|
| 11 | MongoDB | `mongodb` | Spark connector | BaseExtractor | ✅ |
| 12 | Cassandra | `cassandra` | Spark connector | BaseExtractor/BaseLoader | ✅ |
| 13 | DynamoDB | `dynamodb` | boto3 + Spark | BaseExtractor | ✅ |
| 14 | Redis | `redis` | redis-py + Spark | BaseExtractor | ✅ |
| 15 | ADLS | `file` | (file plugin kapsar) | — | ✅ |
| 16 | GCS | `file` | (file plugin kapsar) | — | ✅ |
| 17 | Elasticsearch | `elasticsearch` | elasticsearch-py + Spark | BaseExtractor | ✅ |
| 18 | TimescaleDB | `timescaledb` | JDBC (PG driver) | JdbcExtractor/JdbcLoader | ✅ |
| 19 | HDFS | `file` | (file plugin kapsar) | — | ✅ |
| 20 | InfluxDB | `influxdb` | influxdb-client + Spark | BaseExtractor | ✅ |

### 21–30: Emerging Databases & Analytical Tools

| # | DB | Variant | Type |
|---|---|---|---|
| 21 | Apache Druid | `druid` | JDBC |
| 22 | Vertica | `vertica` | JDBC |
| 23 | SingleStore (MemSQL) | `singlestore` | JDBC (MySQL driver) |
| 24 | Exasol | `exasol` | JDBC |
| 25 | SAP HANA | `saphana` | JDBC |
| 26 | IBM Db2 | `db2` | JDBC |
| 27 | Neo4j | `neo4j` | neo4j-driver + Spark |
| 28 | Greenplum | `greenplum` | JDBC (PG driver) |
| 29 | CockroachDB | `cockroachdb` | JDBC (PG driver) |
| 30 | AWS Athena | `athena` | JDBC |

### 31–40: Streaming Systems

| # | System | Variant | Type |
|---|---|---|---|
| 31 | Kafka | `kafka` | Spark structured streaming (micro-batch) |
| 32 | RabbitMQ | `rabbitmq` | pika + Spark |
| 33 | Pulsar | `pulsar` | Spark connector |
| 34 | Apache Flink | `flink` | REST API |
| 35 | Amazon Kinesis | `kinesis` | Spark connector |
| 36 | Google Pub/Sub | `pubsub` | Spark connector |
| 37 | Azure Event Hubs | `eventhubs` | Spark connector |
| 38 | Apache NiFi | `nifi` | REST API |
| 39 | ActiveMQ | `activemq` | JMS + Spark |
| 40 | Redpanda | `redpanda` | Kafka-compatible |

### 41–50: File Formats & Data Lakes

All covered by `file` variant plugin with `extra.format`:
parquet, avro, json, csv, xml, orc, iceberg, delta

| # | Format/Storage | Variant |
|---|---|---|
| 41–46 | Parquet/Avro/JSON/CSV/XML/ORC | `file` (extra.format) |
| 47 | Google Drive | `gdrive` (REST API) |
| 48 | Dropbox | `dropbox` (REST API) |
| 49 | Box | `box` (REST API) |
| 50 | FTP/SFTP | `ftp` (paramiko + Spark) |

### 51–60: ERP/CRM Systems

| # | System | Variant | Type |
|---|---|---|---|
| 51 | Salesforce | `salesforce` | REST API |
| 52 | SAP | `sap` | RFC/REST API |
| 53 | Microsoft Dynamics | `dynamics` | REST API |
| 54 | NetSuite | `netsuite` | REST API |
| 55 | Workday | `workday` | REST API |
| 56 | HubSpot | `hubspot` | REST API |
| 57 | Zoho CRM | `zoho` | REST API |
| 58 | Freshsales | `freshsales` | REST API |
| 59 | Zendesk | `zendesk` | REST API |
| 60 | Oracle NetSuite | `netsuite` | REST API |

### 61–70: Specialized Analytics Tools

| # | Tool | Variant | Type |
|---|---|---|---|
| 61 | Metabase | `metabase` | REST API |
| 62 | Tableau | `tableau` | REST API |
| 63 | Power BI | `powerbi` | REST API |
| 64 | Looker | `looker` | REST API |
| 65 | Google Analytics (GA4) | `ga4` | REST API |
| 66 | Mixpanel | `mixpanel` | REST API |
| 67 | Amplitude | `amplitude` | REST API |
| 68 | Adobe Analytics | `adobe_analytics` | REST API |
| 69 | Heap | `heap` | REST API |
| 70 | Klipfolio | `klipfolio` | REST API |

### 71–80: Industry-Specific Databases

| # | DB | Variant | Type |
|---|---|---|---|
| 71 | Aerospike | `aerospike` | Native SDK |
| 72 | RocksDB | `rocksdb` | Native SDK |
| 73 | FaunaDB | `faunadb` | REST API |
| 74 | ScyllaDB | `scylladb` | Cassandra-compatible |
| 75 | ArangoDB | `arangodb` | REST API |
| 76 | MarkLogic | `marklogic` | REST API |
| 77 | CrateDB | `cratedb` | JDBC (PG wire) |
| 78 | TigerGraph | `tigergraph` | REST API |
| 79 | HarperDB | `harperdb` | REST API |
| 80 | SAP ASE (Sybase) | `sybase` | JDBC |

### 81–90: Legacy Databases

| # | DB | Variant | Type |
|---|---|---|---|
| 81 | Teradata | `teradata` | JDBC |
| 82 | Netezza | `netezza` | JDBC |
| 83 | Informix | `informix` | JDBC |
| 84 | Ingres | `ingres` | JDBC |
| 85 | Firebird | `firebird` | JDBC |
| 86 | Progress OpenEdge | `openedge` | JDBC |
| 87 | ParAccel | `paraccel` | JDBC |
| 88 | MaxDB | `maxdb` | JDBC |
| 89 | HP Vertica | `vertica` | JDBC |
| 90 | Sybase IQ | `sybaseiq` | JDBC |

### 91–100: Emerging Cloud & Hybrid Databases

| # | DB | Variant | Type |
|---|---|---|---|
| 91 | PlanetScale | `planetscale` | JDBC (MySQL wire) |
| 92 | YugabyteDB | `yugabytedb` | JDBC (PG wire) |
| 93 | TiDB | `tidb` | JDBC (MySQL wire) |
| 94 | OceanBase | `oceanbase` | JDBC (MySQL wire) |
| 95 | Citus | `citus` | JDBC (PG wire) |
| 96 | Snowplow Analytics | `snowplow` | REST API |
| 97 | Spanner (Google Cloud) | `spanner` | JDBC |
| 98 | MariaDB ColumnStore | `mariadb` | JDBC (MariaDB driver) |
| 99 | CockroachDB Serverless | `cockroachdb` | JDBC (PG wire) |
| 100 | Weaviate | `weaviate` | REST API |

---

## Architecture Notes

### Plugin Hierarchy

```
BaseExtractor (ABC)
    ├── JdbcExtractor (JDBC common — _jdbc_options() hook)
    │       ├── PostgresExtractor (variant='postgresql')
    │       ├── MysqlExtractor (variant='mysql')
    │       ├── SnowflakeExtractor (variant='snowflake') — RSA key via _jdbc_options()
    │       ├── ... (all JDBC databases)
    │       └── TeradataExtractor (variant='teradata')
    ├── FileExtractor (variant='file') — S3/ADLS/GCS/HDFS/local + parquet/iceberg/delta/csv/json
    ├── MongoExtractor (variant='mongodb') — mongo-spark-connector
    ├── CassandraExtractor (variant='cassandra') — spark-cassandra-connector
    ├── DynamoDBExtractor (variant='dynamodb') — boto3 + createDataFrame
    ├── RedisExtractor (variant='redis') — redis-py + createDataFrame
    ├── ElasticsearchExtractor (variant='elasticsearch') — elasticsearch-py + createDataFrame
    ├── InfluxDBExtractor (variant='influxdb') — influxdb-client + createDataFrame
    └── BigQueryExtractor (variant='bigquery') — spark-bigquery-connector
```

### `file` Variant — Unified Storage + Format

Replaces old `mkpipe-extractor-s3` / `mkpipe-loader-s3`. Covers:
- **Storage**: S3, ADLS, GCS, HDFS, local (via `extra.storage`)
- **Format**: parquet, iceberg, delta, csv, json, orc, avro (via `extra.format`)
- **Iceberg**: catalog-based, ACID, schema evolution, time travel

### Streaming (Phase 3+)

Micro-batch via `BaseExtractor` works now. Full streaming requires `BaseStreamExtractor` ABC.

### REST API / SaaS (Phase 5+)

All use `BaseExtractor` directly. `ConnectionConfig` fields: `api_key`, `oauth_token`, `client_id`, `client_secret`, `credentials_file`.
