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


class DuckDBBackend(BackendBase, variant='duckdb'):
    def __init__(self, config: Dict[str, Any]):
        self.db_path = config.get('database', 'mkpipe_manifest.duckdb')
        super().__init__(config)

    def _connect(self):
        import duckdb
        return duckdb.connect(self.db_path)

    def init_table(self) -> None:
        conn = self._connect()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mkpipe_manifest (
                    pipeline_name VARCHAR,
                    table_name VARCHAR,
                    last_point VARCHAR,
                    type VARCHAR,
                    replication_method VARCHAR,
                    status VARCHAR,
                    error_message VARCHAR,
                    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (pipeline_name, table_name)
                )
            """)
        finally:
            conn.close()

    @_retry()
    def get_table_status(self, pipeline_name: str, table_name: str) -> Optional[str]:
        conn = self._connect()
        try:
            result = conn.execute(
                'SELECT status, updated_time FROM mkpipe_manifest '
                'WHERE pipeline_name = ? AND table_name = ?',
                [pipeline_name, table_name],
            ).fetchone()

            if not result:
                return None

            status, updated_time = result
            if updated_time and (datetime.now() - updated_time > timedelta(days=1)):
                conn.execute(
                    'UPDATE mkpipe_manifest SET status = ?, '
                    'updated_time = CURRENT_TIMESTAMP '
                    'WHERE pipeline_name = ? AND table_name = ?',
                    ['failed', pipeline_name, table_name],
                )
                return 'failed'

            return status
        finally:
            conn.close()

    @_retry()
    def get_last_point(self, pipeline_name: str, table_name: str) -> Optional[str]:
        conn = self._connect()
        try:
            result = conn.execute(
                'SELECT last_point FROM mkpipe_manifest '
                'WHERE pipeline_name = ? AND table_name = ?',
                [pipeline_name, table_name],
            ).fetchone()
            return result[0] if result else None
        finally:
            conn.close()

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
        conn = self._connect()
        try:
            result = conn.execute(
                'SELECT 1 FROM mkpipe_manifest '
                'WHERE pipeline_name = ? AND table_name = ?',
                [pipeline_name, table_name],
            ).fetchone()

            if result:
                update_fields = []
                update_values = []

                if value is not None:
                    update_fields.append('last_point = ?')
                    update_values.append(value)
                if value_type is not None:
                    update_fields.append('type = ?')
                    update_values.append(value_type)

                update_fields.extend([
                    'status = ?',
                    'replication_method = ?',
                    'error_message = ?',
                    'updated_time = CURRENT_TIMESTAMP',
                ])
                update_values.extend([
                    status, replication_method, error_message,
                    pipeline_name, table_name,
                ])

                sql = (
                    f"UPDATE mkpipe_manifest SET {', '.join(update_fields)} "
                    f"WHERE pipeline_name = ? AND table_name = ?"
                )
                conn.execute(sql, update_values)
            else:
                conn.execute(
                    'INSERT INTO mkpipe_manifest '
                    '(pipeline_name, table_name, last_point, type, status, '
                    'replication_method, error_message, updated_time) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)',
                    [
                        pipeline_name, table_name, value, value_type,
                        status, replication_method, error_message,
                    ],
                )
        finally:
            conn.close()
