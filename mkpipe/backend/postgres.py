from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from .base import BackendBase, retry


class PostgresBackend(BackendBase, variant='postgresql'):
    def __init__(self, config: Dict[str, Any]):
        self.connection_params = config
        super().__init__(config)

    def _connect(self):
        import psycopg2
        return psycopg2.connect(
            database=self.connection_params.get('database'),
            user=self.connection_params.get('user'),
            password=self.connection_params.get('password'),
            host=self.connection_params.get('host'),
            port=self.connection_params.get('port'),
        )

    def init_table(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS mkpipe_manifest (
                        pipeline_name VARCHAR(255),
                        table_name VARCHAR(255),
                        last_point VARCHAR(50),
                        type VARCHAR(50),
                        replication_method VARCHAR(20)
                            CHECK (replication_method IN ('incremental', 'full')),
                        status VARCHAR(20)
                            CHECK (status IN (
                                'completed', 'failed', 'extracting', 'loading'
                            )),
                        error_message TEXT,
                        updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (pipeline_name, table_name)
                    )
                """)
                conn.commit()

    @retry()
    def get_table_status(self, pipeline_name: str, table_name: str) -> Optional[str]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT status, updated_time FROM mkpipe_manifest '
                    'WHERE pipeline_name = %s AND table_name = %s',
                    (pipeline_name, table_name),
                )
                result = cursor.fetchone()
                if not result:
                    return None

                status, updated_time = result
                if updated_time and (datetime.now() - updated_time > timedelta(days=1)):
                    cursor.execute(
                        'UPDATE mkpipe_manifest SET status = %s, '
                        'updated_time = CURRENT_TIMESTAMP '
                        'WHERE pipeline_name = %s AND table_name = %s',
                        ('failed', pipeline_name, table_name),
                    )
                    conn.commit()
                    return 'failed'

                return status

    @retry()
    def get_last_point(self, pipeline_name: str, table_name: str) -> Optional[str]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT last_point FROM mkpipe_manifest '
                    'WHERE pipeline_name = %s AND table_name = %s',
                    (pipeline_name, table_name),
                )
                result = cursor.fetchone()
                return result[0] if result else None

    @retry()
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
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT 1 FROM mkpipe_manifest '
                    'WHERE pipeline_name = %s AND table_name = %s',
                    (pipeline_name, table_name),
                )
                exists = cursor.fetchone()

                if exists:
                    update_fields = []
                    update_values = []

                    if value is not None:
                        update_fields.append('last_point = %s')
                        update_values.append(value)
                    if value_type is not None:
                        update_fields.append('type = %s')
                        update_values.append(value_type)

                    update_fields.extend([
                        'status = %s',
                        'replication_method = %s',
                        'error_message = %s',
                        'updated_time = CURRENT_TIMESTAMP',
                    ])
                    update_values.extend([
                        status, replication_method, error_message,
                        pipeline_name, table_name,
                    ])

                    sql = (
                        f"UPDATE mkpipe_manifest SET {', '.join(update_fields)} "
                        f"WHERE pipeline_name = %s AND table_name = %s"
                    )
                    cursor.execute(sql, tuple(update_values))
                else:
                    cursor.execute(
                        'INSERT INTO mkpipe_manifest '
                        '(pipeline_name, table_name, last_point, type, status, '
                        'replication_method, error_message, updated_time) '
                        'VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)',
                        (
                            pipeline_name, table_name, value, value_type,
                            status, replication_method, error_message,
                        ),
                    )
                conn.commit()
