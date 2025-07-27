import os
import sqlite3
import time
from datetime import datetime, timedelta
from functools import wraps
from ..utils import Logger
from ..config import ROOT_DIR


def retry_on_failure(max_attempts=5, delay=1):
    logger = Logger(__file__)
    """
    Decorator to retry a database operation upon failure.

    Parameters:
        max_attempts (int): Maximum number of retry attempts.
        delay (int): Delay between retries in seconds.

    Returns:
        decorator: A decorator to apply retry logic to a function.
    """

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


class ConnectorSQLite:
    def __init__(self, connection_params):
        self.connection_params = connection_params
        self.db_path = os.path.abspath(os.path.join(ROOT_DIR, 'artifacts', 'sqlite.db'))

        # Ensure the directory for the database exists
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

    def connect(self):
        return sqlite3.connect(
            self.db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )

    @retry_on_failure()
    def get_table_status(self, pipeline_name, table_name):
        """
        Check the status of a table from the mkpipe_manifest. If the updated_time is older than 1 day,
        update the status to 'failed' and return 'failed'. Otherwise, return the current status.
        If the table does not exist, create it first.
        """
        with self.connect() as conn:
            conn.row_factory = sqlite3.Row
            with conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS mkpipe_manifest (
                        pipeline_name TEXT,
                        table_name TEXT,
                        last_point TEXT,
                        type TEXT,
                        replication_method TEXT CHECK (replication_method IN ('incremental', 'full')),
                        status TEXT CHECK (status IN ('completed', 'failed', 'extracting', 'loading', 'extracted', 'loaded')),
                        error_message TEXT,
                        updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )

                result = conn.execute(
                    'SELECT status, updated_time FROM mkpipe_manifest WHERE table_name = ? and pipeline_name = ?',
                    (
                        table_name,
                        pipeline_name,
                    ),
                ).fetchone()

                if result:
                    current_status, updated_time = result
                    time_diff = datetime.now() - updated_time
                    if time_diff > timedelta(days=1):
                        conn.execute(
                            'UPDATE mkpipe_manifest SET status = ?, updated_time = CURRENT_TIMESTAMP WHERE table_name = ? and pipeline_name = ?',
                            (
                                'failed',
                                table_name,
                                pipeline_name,
                            ),
                        )
                        return 'failed'
                    else:
                        return current_status
                else:
                    return None

    @retry_on_failure()
    def get_last_point(self, pipeline_name, table_name):
        """
        Retrieve the last_point value for the given table.
        """
        with self.connect() as conn:
            conn.row_factory = sqlite3.Row
            with conn:
                result = conn.execute(
                    'SELECT last_point FROM mkpipe_manifest WHERE table_name = ? and pipeline_name = ?',
                    (
                        table_name,
                        pipeline_name,
                    ),
                ).fetchone()
                return result[0] if result else None

    @retry_on_failure()
    def manifest_table_update(
        self,
        pipeline_name,
        table_name,
        value,
        value_type,
        status='completed',
        replication_method='full',
        error_message=None,
    ):
        """
        Update or insert the last point value, value type, status, and error message for a specified table.
        """
        with self.connect() as conn:
            conn.row_factory = sqlite3.Row
            with conn:
                exists = conn.execute(
                    'SELECT 1 FROM mkpipe_manifest WHERE table_name = ? and pipeline_name = ?',
                    (table_name, pipeline_name),
                ).fetchone()
                if exists:
                    # Prepare update fields
                    update_fields = []
                    update_values = []

                    if value is not None:
                        update_fields.append('last_point = ?')
                        update_values.append(value)

                    if value_type is not None:
                        update_fields.append('type = ?')
                        update_values.append(value_type)

                    # Always update status, replication_method, and error_message

                    update_fields.extend(
                        [
                            'status = ?',
                            'replication_method = ?',
                            'error_message = ?',  # Update to clear old errors
                            'updated_time = CURRENT_TIMESTAMP',
                        ]
                    )
                    update_values.extend(
                        [status, replication_method, error_message, table_name, pipeline_name]
                    )

                    # Construct and execute the update query
                    update_query = f"""
                        UPDATE mkpipe_manifest
                        SET {', '.join(update_fields)}
                        WHERE table_name = ? and pipeline_name = ?
                    """
                    conn.execute(update_query, tuple(update_values))
                else:
                    # Insert new entry
                    conn.execute(
                        """
                        INSERT INTO mkpipe_manifest (
                            pipeline_name, table_name, last_point, type, status, 
                            replication_method, error_message, updated_time
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (
                            pipeline_name,
                            table_name,
                            value,
                            value_type,
                            status,
                            replication_method,
                            error_message,
                        ),
                    )
