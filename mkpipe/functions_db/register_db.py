from .connector_postgresql import ConnectorPostgresql
from .connector_sqlite import ConnectorSQLite
from .connector_duckdb import ConnectorDuckDB
from .connector_clickhouse import ConnectorClickhouse

DB_CONNECTIONS = {}


def register_db_connector(db_type):
    def decorator(fn):
        DB_CONNECTIONS[db_type] = fn
        return fn

    return decorator


@register_db_connector('postgresql')
def connector_postgresql(connection_params):
    return ConnectorPostgresql(connection_params)


@register_db_connector('sqlite')
def connector_sqlite(connection_params):
    return ConnectorSQLite(connection_params)


@register_db_connector('duckdb')
def connector_duckdb(connection_params):
    return ConnectorDuckDB(connection_params)


@register_db_connector('clickhouse')
def connector_clickhouse(connection_params):
    return ConnectorClickhouse(connection_params)


def get_db_connector(variant):
    if variant not in DB_CONNECTIONS:
        raise ValueError(f'Unsupported connector type: {variant}')
    return DB_CONNECTIONS.get(variant)
