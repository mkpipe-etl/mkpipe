# MkPipe

**MkPipe** is a modular, open-source ETL (Extract, Transform, Load) tool that allows you to integrate various data sources and sinks easily. It is designed to be extensible with a plugin-based architecture that supports extractors, transformers, and loaders.

## Features

- Extract data from multiple sources (e.g., PostgreSQL, MongoDB).
- Transform data using custom Python logic and Apache Spark.
- Load data into various sinks (e.g., ClickHouse, PostgreSQL, Parquet).
- Plugin-based architecture that supports future extensions.
- Cloud-native architecture, can be deployed on Kubernetes and other environments.

## Installation

You can install the core package and extractors using pip:

### Install the core package:
```bash
pip install mkpipe
```

### Install the Postgres extractor:
```bash
pip install mkpipe-extractor-postgres
```

### Install additional extractors or loaders as needed:
You can find or contribute new extractors and loaders in the future.

## Usage

To run the ETL process, use the following command:

```py
from mkpipe_core.plugins.registry import EXTRACTORS

def test_postgres_extractor():
    postgres_extractor = EXTRACTORS.get("postgres")
    if not postgres_extractor:
        print("Postgres extractor not found!")
        return
    instance = postgres_extractor()
    instance.extract()

if __name__ == "__main__":
    test_postgres_extractor()

```

Where `elt.yaml` is your configuration file that specifies the extractors, transformers, and loaders.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/m-karakus/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

