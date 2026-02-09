import time
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, Optional

from .base import BackendBase


def _retry(max_attempts=5, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts >= max_attempts:
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator


class ClickhouseBackend(BackendBase, variant='clickhouse'):
    def __init__(self, config: Dict[str, Any]):
        self.connection_params = config
        super().__init__(config)

    def _connect(self):
        import clickhouse_connect
        return clickhouse_connect.get_client(
            host=self.connection_params.get('host', 'localhost'),
            port=int(self.connection_params.get('port', 8123)),
            username=self.connection_params.get('user', 'default'),
            password=self.connection_params.get('password', ''),
            database=self.connection_params.get('database', 'default'),
        )

    def init_table(self) -> None:
        client = self._connect()
        client.command("""
            CREATE TABLE IF NOT EXISTS mkpipe_manifest (
                pipeline_name String,
                table_name String,
                last_point Nullable(String),
                type Nullable(String),
                replication_method Nullable(String),
                status Nullable(String),
                error_message Nullable(String),
                updated_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_time)
            ORDER BY (pipeline_name, table_name)
        """)

    @_retry()
    def get_table_status(self, pipeline_name: str, table_name: str) -> Optional[str]:
        client = self._connect()
        result = client.query(
            'SELECT status, updated_time FROM mkpipe_manifest FINAL '
            'WHERE pipeline_name = {p_name:String} AND table_name = {t_name:String}',
            parameters={'p_name': pipeline_name, 't_name': table_name},
        )
        if not result.result_rows:
            return None

        status, updated_time = result.result_rows[0]
        if updated_time and (datetime.now() - updated_time > timedelta(days=1)):
            client.command(
                'INSERT INTO mkpipe_manifest '
                '(pipeline_name, table_name, status, updated_time) VALUES',
                data=[(pipeline_name, table_name, 'failed', datetime.now())],
            )
            return 'failed'

        return status

    @_retry()
    def get_last_point(self, pipeline_name: str, table_name: str) -> Optional[str]:
        client = self._connect()
        result = client.query(
            'SELECT last_point FROM mkpipe_manifest FINAL '
            'WHERE pipeline_name = {p_name:String} AND table_name = {t_name:String}',
            parameters={'p_name': pipeline_name, 't_name': table_name},
        )
        if not result.result_rows:
            return None
        return result.result_rows[0][0]

    @_retry()
    def manifest_table_update(
        self,
        pipeline_name: str,
        table_name: str,
        value: Optional[str] = None,
        value_type: Optional[str] = None,
        status: str = 'completed',
        replication_method: str = 'full',
        error_message: Optional[str] = None,
    ) -> None:
        client = self._connect()

        existing = client.query(
            'SELECT last_point, type FROM mkpipe_manifest FINAL '
            'WHERE pipeline_name = {p_name:String} AND table_name = {t_name:String}',
            parameters={'p_name': pipeline_name, 't_name': table_name},
        )

        if existing.result_rows:
            current_last_point, current_type = existing.result_rows[0]
            final_value = value if value is not None else current_last_point
            final_type = value_type if value_type is not None else current_type
        else:
            final_value = value
            final_type = value_type

        client.insert(
            'mkpipe_manifest',
            data=[[
                pipeline_name, table_name, final_value, final_type,
                replication_method, status, error_message, datetime.now(),
            ]],
            column_names=[
                'pipeline_name', 'table_name', 'last_point', 'type',
                'replication_method', 'status', 'error_message', 'updated_time',
            ],
        )
