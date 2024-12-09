
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

1. Clone or copy the `deploy` folder from the repository.
2. Modify the configuration files:
   - `.env` for environment variables.
   - `mkpipe_project.yaml` for your specific ETL configurations.
3. Run the following command to start the services:
   ```bash
   docker-compose up --build
   ```
   This will set up the following services:
   - PostgreSQL: Required for data storage.
   - RabbitMQ: Required for the Celery `run_coordinator=celery`.
   - Celery Worker: Required for running the Celery `run_coordinator=celery`.
   - Flower UI: Optional, but required for monitoring Celery tasks.

   **Note:** If you only want to use the `run_coordinator=single` without Celery, only PostgreSQL is necessary.

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

For more detailed documentation, please visit the [GitHub repository](https://github.com/m-karakus/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
