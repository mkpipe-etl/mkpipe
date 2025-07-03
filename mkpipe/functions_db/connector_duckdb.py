import os
import duckdb
import time
from datetime import datetime, timedelta
from functools import wraps
from ..utils import Logger
from ..config import ROOT_DIR


def retry_on_failure(max_attempts=5, delay=1):
    logger = Logger(__file__)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts < max_attempts:
                        logger.info(
                            {
                                'message': f'Attempt {attempts} failed: {e}. Retrying in {delay} seconds...'
                            }
                        )
                        time.sleep(delay)
                    else:
                        logger.error({'message': f'Error after {max_attempts} attempts: {e}'})
                        raise e

        return wrapper

    return decorator


class ConnectorDuckDB:
    def __init__(self, connection_params):
        self.connection_params = connection_params
        self.db_path = os.path.abspath(os.path.join(ROOT_DIR, 'artifacts', 'duckdb.db'))

        # Ensure the directory for the database exists
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

    def connect(self):
        return duckdb.connect(self.db_path)

    @retry_on_failure()
    def get_table_status(self, name):
        with self.connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mkpipe_manifest (
                    table_name TEXT PRIMARY KEY,
                    last_point TEXT,
                    type TEXT,
                    replication_method TEXT CHECK (replication_method IN ('incremental', 'full')),
                    status TEXT CHECK (status IN ('completed', 'failed', 'extracting', 'loading', 'extracted', 'loaded')),
                    error_message TEXT,
                    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            result = conn.execute(
                'SELECT status, updated_time FROM mkpipe_manifest WHERE table_name = ?',
                (name,),
            ).fetchone()

            if result:
                current_status, updated_time = result
                time_diff = datetime.now() - updated_time
                if time_diff > timedelta(days=1):
                    conn.execute(
                        'UPDATE mkpipe_manifest SET status = ?, updated_time = CURRENT_TIMESTAMP WHERE table_name = ?',
                        ('failed', name),
                    )
                    return 'failed'
                else:
                    return current_status
            else:
                return None

    @retry_on_failure()
    def get_last_point(self, name):
        with self.connect() as conn:
            result = conn.execute(
                'SELECT last_point FROM mkpipe_manifest WHERE table_name = ?', (name,)
            ).fetchone()
            return result[0] if result else None

    @retry_on_failure()
    def manifest_table_update(
        self,
        name,
        value,
        value_type,
        status='completed',
        replication_method='full',
        error_message=None,
    ):
        with self.connect() as conn:
            exists = conn.execute(
                'SELECT 1 FROM mkpipe_manifest WHERE table_name = ?', (name,)
            ).fetchone()

            if exists:
                update_fields = []
                update_values = []

                if value is not None:
                    update_fields.append('last_point = ?')
                    update_values.append(value)

                if value_type is not None:
                    update_fields.append('type = ?')
                    update_values.append(value_type)

                update_fields.append('status = ?')
                update_values.append(status)

                update_fields.append('replication_method = ?')
                update_values.append(replication_method)

                if error_message is not None:
                    update_fields.append('error_message = ?')
                    update_values.append(error_message)

                update_fields.append('updated_time = CURRENT_TIMESTAMP')

                update_values.append(name)

                conn.execute(
                    f"""
                    UPDATE mkpipe_manifest
                    SET {', '.join(update_fields)}
                    WHERE table_name = ?
                """,
                    update_values,
                )
            else:
                conn.execute(
                    """
                    INSERT INTO mkpipe_manifest (
                        table_name, last_point, type, status, replication_method, error_message, updated_time
                    ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                    (
                        name,
                        value,
                        value_type,
                        status,
                        replication_method,
                        error_message,
                    ),
                )
