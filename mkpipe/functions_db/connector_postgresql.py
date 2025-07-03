import psycopg2
import time
from datetime import datetime, timedelta
from functools import wraps
from ..utils import Logger


def retry_on_failure(max_attempts=5, delay=1):
    """
    Decorator to retry a database operation upon failure.

    Parameters:
        max_attempts (int): Maximum number of retry attempts.
        delay (int): Delay between retries in seconds.

    Returns:
        decorator: A decorator to apply retry logic to a function.
    """

    def decorator(func):
        logger = Logger(__file__)

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


class ConnectorPostgresql:
    def __init__(self, connection_params):
        self.connection_params = connection_params

    def connect(self):
        return psycopg2.connect(
            database=self.connection_params['database'],
            user=self.connection_params['user'],
            password=self.connection_params['password'],
            host=self.connection_params['host'],
            port=self.connection_params['port'],
        )

    @retry_on_failure()
    def get_table_status(self, name):
        """
        Check the status of a table from the mkpipe_manifest. If the updated_time is older than 1 day,
        update the status to 'failed' and return 'failed'. Otherwise, return the current status.
        If the table does not exist, create it first.
        """
        with self.connect() as conn:
            # Ensure the mkpipe_manifest table exists
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS mkpipe_manifest (
                        table_name VARCHAR(255) PRIMARY KEY,
                        last_point VARCHAR(50),
                        type VARCHAR(50),
                        replication_method VARCHAR(20) CHECK (replication_method IN ('incremental', 'full')),
                        status VARCHAR(20) CHECK (status IN ('completed', 'failed', 'extracting', 'loading', 'extracted', 'loaded')),
                        error_message TEXT,
                        updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                conn.commit()

                # Check table status
                cursor.execute(
                    'SELECT status, updated_time FROM mkpipe_manifest WHERE table_name = %s',
                    (name,),
                )
                result = cursor.fetchone()

                if result:
                    current_status, updated_time = result
                    time_diff = datetime.now() - updated_time

                    if time_diff > timedelta(days=1):
                        cursor.execute(
                            'UPDATE mkpipe_manifest SET status = %s, updated_time = CURRENT_TIMESTAMP WHERE table_name = %s',
                            ('failed', name),
                        )
                        conn.commit()
                        return 'failed'
                    else:
                        return current_status
                else:
                    return None

    @retry_on_failure()
    def get_last_point(self, name):
        """
        Retrieve the last_point value for the given table.
        """
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT last_point FROM mkpipe_manifest WHERE table_name = %s',
                    (name,),
                )
                result = cursor.fetchone()
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
        """
        Update or insert the last point value, value type, status, and error message for a specified table.
        """
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT table_name FROM mkpipe_manifest WHERE table_name = %s',
                    (name,),
                )
                if cursor.fetchone():
                    # Prepare update fields
                    update_fields = []
                    update_values = []

                    if value is not None:
                        update_fields.append('last_point = %s')
                        update_values.append(value)

                    if value_type is not None:
                        update_fields.append('type = %s')
                        update_values.append(value_type)

                    # Always update status, replication_method, and error_message
                    update_fields.append('status = %s')
                    update_values.append(status)

                    update_fields.append('replication_method = %s')
                    update_values.append(replication_method)

                    if error_message is not None:
                        update_fields.append('error_message = %s')
                        update_values.append(error_message)

                    update_fields.append('updated_time = CURRENT_TIMESTAMP')

                    update_values.append(name)  # For the WHERE clause

                    # Construct and execute the update query
                    update_sql = f"""
                        UPDATE mkpipe_manifest
                        SET {', '.join(update_fields)}
                        WHERE table_name = %s
                    """
                    cursor.execute(update_sql, tuple(update_values))
                else:
                    # Insert new entry
                    cursor.execute(
                        """
                        INSERT INTO mkpipe_manifest (table_name, last_point, type, status, replication_method, error_message, updated_time)
                        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
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
                conn.commit()
