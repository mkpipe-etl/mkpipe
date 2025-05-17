
# MkPipe

**MkPipe** is a modular, open-source ETL (Extract, Transform, Load) tool that allows you to integrate various data sources and sinks easily. It is designed to be extensible with a plugin-based architecture that supports extractors, transformers, and loaders.

## Features

- Extract data from multiple sources (e.g., PostgreSQL, MongoDB).
- Transform data using custom Python logic and Apache Spark.
- Load data into various sinks (e.g., ClickHouse, PostgreSQL, Parquet).
- Plugin-based architecture that supports future extensions.
- Cloud-native architecture, can be deployed on Kubernetes and other environments.

## Quick Setup

You can deploy MkPipe using one of the following strategies:

### 1. Using Docker Compose

This method sets up all required services automatically using Docker Compose.

#### Steps:

1. Clone or copy the [`deploy`](./deploy/single) folder from the repository.
2. Modify the configuration files:
   - [`.env`](./deploy/celery/.env.example) for environment variables.
   - [`mkpipe_project.yaml`](./deploy/celery/mkpipe_project.yaml.example) for your specific ETL configurations.
3. Run the following command to start the services:
   ```bash
   docker-compose up --build
   ```
   This will set up the following services:
   - PostgreSQL: Required for data storage.
   - RabbitMQ: Required for the Celery [`run_coordinator=celery`](./deploy/celery/mkpipe_project.yaml.example#L7).
   - Celery Worker: Required for running the Celery [`run_coordinator=celery`](./deploy/celery/mkpipe_project.yaml.example#L7).
   - Flower UI: Optional, but required for monitoring Celery tasks.

   **Note:** If you only want to use the [`run_coordinator=single`](./deploy/single/mkpipe_project.yaml.example#L7)without Celery, only PostgreSQL is necessary.

### 2. Running Locally

You can also set up the environment manually and run MkPipe locally.

#### Steps:

1. Set up and configure the following services:
   - RabbitMQ: Required for the Celery `run_coordinator`.
   - PostgreSQL: Required for data storage.
   - Flower UI: Optional, but required for monitoring Celery tasks.
2. Update the following configuration files in the `deploy` folder:
   - `.env` for environment variables.
   - `mkpipe_project.yaml` for your ETL configurations.
3. Install the python packages
   ```bash
   pip install mkpipe mkpipe-extractor-postgres mkpipe-loader-postgres
   ```
4. Set the project directory environment variable:
   ```bash
   export MKPIPE_PROJECT_DIR={YOUR_PROJECT_PATH}
   ```
5. Start MkPipe using the following command:
   ```bash
   mkpipe run
   ```

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.


# Db Support Plan 

**For actively supported databases/plugins, please visit the [MkPipe-hub repository!](https://github.com/mkpipe-etl/mkpipe-hub)**

## Core Relational Databases  
- [x]  PostgreSQL
- [x]  MySQL
- [x]  MariaDB
- [x]  SQL Server
- [x]  Oracle Database
- [x]  SQLite
- [ ]  Snowflake
- [ ]  Google BigQuery
- [x]  Amazon Redshift
- [x]  ClickHouse
- [x]  Amazon S3
---
## NoSQL Databases  
- [ ]  MongoDB
- [ ]  Cassandra
- [ ]  DynamoDB
- [ ]  Redis
- [ ]  Azure Data Lake Storage (ADLS)
- [ ]  Google Cloud Storage
- [ ]  Elasticsearch
- [ ]  TimescaleDB
- [ ]  HDFS
- [ ]  InfluxDB
---
## ERP/CRM Systems
- [ ]  Salesforce  
- [ ]  SAP  
- [ ]  Microsoft Dynamics  
- [ ]  NetSuite  
- [ ]  Workday  
- [ ]  HubSpot  
- [ ]  Zoho CRM  
- [ ]  Freshsales  
- [ ]  Zendesk  
- [ ]  Oracle NetSuite  
---
## Emerging Databases & Analytical Tools
Apache Druid  
- [ ]  Vertica  
- [ ]  SingleStore (MemSQL)  
- [ ]  Exasol  
- [ ]  SAP HANA  
- [ ]  IBM Db2  
- [ ]  Neo4j (Graph Database)  
- [ ]  Greenplum  
- [ ]  CockroachDB  
- [ ]  AWS Athena  
---
## Streaming Systems
- [ ]  Kafka  
- [ ]  RabbitMQ  
- [ ]  Pulsar  
- [ ]  Apache Flink  
- [ ]  Amazon Kinesis  
- [ ]  Google Pub/Sub  
- [ ]  Azure Event Hubs  
- [ ]  Apache NiFi  
- [ ]  ActiveMQ  
- [ ]  Redpanda  
---
## File Formats & Data Lakes
- [ ]  Parquet  
- [ ]  Avro  
- [ ]  JSON  
- [ ]  CSV  
- [ ]  XML  
- [ ]  ORC  
- [ ]  Google Drive (for raw files)  
- [ ]  Dropbox  
- [ ]  Box  
- [ ]  FTP/SFTP Servers  
---
## Specialized Analytics Tools
- [ ]  Metabase (Data Visualization)  
- [ ]  Tableau Data Extracts  
- [ ]  Power BI  
- [ ]  Looker  
- [ ]  Google Analytics (GA4)  
- [ ]  Mixpanel  
- [ ]  Amplitude  
- [ ]  Adobe Analytics  
- [ ]  Heap  
- [ ]  Klipfolio  
---
## Industry-Specific Databases
- [ ]  Aerospike  
- [ ]  RocksDB  
- [ ]  FaunaDB  
- [ ]  ScyllaDB  
- [ ]  ArangoDB  
- [ ]  MarkLogic  
- [ ]  CrateDB  
- [ ]  TigerGraph  
- [ ]  HarperDB  
- [ ]  SAP ASE (Sybase)  
---
## Legacy Databases
- [ ]  Teradata  
- [ ]  Netezza  
- [ ]  Informix  
- [ ]  Ingres  
- [ ]  Firebird  
- [ ]  Progress OpenEdge  
- [ ]  ParAccel  
- [ ]  MaxDB  
- [ ]  HP Vertica  
- [ ]  Sybase IQ  
---
## Emerging Cloud & Hybrid Databases
- [ ]  PlanetScale (MySQL-based)  
- [ ]  YugabyteDB  
- [ ]  TiDB  
- [ ]  OceanBase  
- [ ]  Citus (PostgreSQL-based)  
- [ ]  Snowplow Analytics  
- [ ]  Spanner (Google Cloud)  
- [ ]  MariaDB ColumnStore  
- [ ]  CockroachDB Serverless
- [ ]  Weaviate (Vector Search)  
---