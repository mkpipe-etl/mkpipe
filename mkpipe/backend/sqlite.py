import sqlite3
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


class SqliteBackend(BackendBase, variant='sqlite'):
    def __init__(self, config: Dict[str, Any]):
        self.db_path = config.get('database', 'mkpipe_manifest.db')
        super().__init__(config)

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def init_table(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mkpipe_manifest (
                    pipeline_name TEXT,
                    table_name TEXT,
                    last_point TEXT,
                    type TEXT,
                    replication_method TEXT,
                    status TEXT,
                    error_message TEXT,
                    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (pipeline_name, table_name)
                )
            """)
            conn.commit()

    @_retry()
    def get_table_status(self, pipeline_name: str, table_name: str) -> Optional[str]:
        with self._connect() as conn:
            cursor = conn.execute(
                'SELECT status, updated_time FROM mkpipe_manifest '
                'WHERE pipeline_name = ? AND table_name = ?',
                (pipeline_name, table_name),
            )
            result = cursor.fetchone()
            if not result:
                return None

            status, updated_time = result
            if updated_time:
                if isinstance(updated_time, str):
                    try:
                        updated_time = datetime.strptime(
                            updated_time, '%Y-%m-%d %H:%M:%S'
                        )
                    except ValueError:
                        updated_time = datetime.strptime(
                            updated_time, '%Y-%m-%d %H:%M:%S.%f'
                        )

                if datetime.now() - updated_time > timedelta(days=1):
                    conn.execute(
                        'UPDATE mkpipe_manifest SET status = ?, updated_time = CURRENT_TIMESTAMP '
                        'WHERE pipeline_name = ? AND table_name = ?',
                        ('failed', pipeline_name, table_name),
                    )
                    conn.commit()
                    return 'failed'

            return status

    @_retry()
    def get_last_point(self, pipeline_name: str, table_name: str) -> Optional[str]:
        with self._connect() as conn:
            cursor = conn.execute(
                'SELECT last_point FROM mkpipe_manifest '
                'WHERE pipeline_name = ? AND table_name = ?',
                (pipeline_name, table_name),
            )
            result = cursor.fetchone()
            return result[0] if result else None

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
        with self._connect() as conn:
            cursor = conn.execute(
                'SELECT 1 FROM mkpipe_manifest '
                'WHERE pipeline_name = ? AND table_name = ?',
                (pipeline_name, table_name),
            )
            exists = cursor.fetchone()

            if exists:
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
                conn.execute(sql, tuple(update_values))
            else:
                conn.execute(
                    'INSERT INTO mkpipe_manifest '
                    '(pipeline_name, table_name, last_point, type, status, '
                    'replication_method, error_message, updated_time) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)',
                    (
                        pipeline_name, table_name, value, value_type,
                        status, replication_method, error_message,
                    ),
                )
            conn.commit()
