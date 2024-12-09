import psycopg2
import time
from datetime import datetime, timedelta
from functools import wraps
from .register_db import register_db_connector, get_db_connector
from ..config import load_config
from ..utils import Logger

logger = Logger(__file__)


@register_db_connector('postgresql')
def connect_postgresql(connection_params):
    """
    Establish a connection to a PostgreSQL database using provided connection parameters.

    Parameters:
        connection_params (dict): Dictionary containing database connection parameters.

    Returns:
        connection: A connection object for the PostgreSQL database.
    """
    return psycopg2.connect(
        database=connection_params['database'],
        user=connection_params['user'],
        password=connection_params['password'],
        host=connection_params['host'],
        port=connection_params['port'],
    )


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
                        logger.error(
                            {'message': f'Error after {max_attempts} attempts: {e}'}
                        )
                        raise e

        return wrapper

    return decorator


@retry_on_failure()
def get_table_status(name):
    """
    Check the status of a table from the mkpipe_manifest. If the updated_time is older than 1 day,
    update the status to 'failed' and return 'failed'. Otherwise, return the current status.
    If the table does not exist, create it first.
    """
    config = load_config()
    connection_params = config['settings']['backend']
    db_type = config['settings']['backend']['database_type']
    db_connector = get_db_connector(db_type)

    with db_connector(connection_params) as conn:
        # Ensure the mkpipe_manifest table exists
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS mkpipe_manifest (
                    table_name VARCHAR(255) NOT NULL,
                    last_point VARCHAR(50),
                    type VARCHAR(50),
                    replication_method VARCHAR(20) CHECK (replication_method IN ('incremental', 'full')),
                    status VARCHAR(20) CHECK (status IN ('completed', 'failed', 'extracting', 'loading', 'extracted', 'loaded')),
                    error_message TEXT,
                    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_table_name UNIQUE (table_name)  -- Unique constraint on table_name
                );
            """)
            conn.commit()

        # Check table status
        with conn.cursor() as cursor:
            cursor.execute(
                'SELECT status, updated_time FROM mkpipe_manifest WHERE table_name = %s',
                (name,),
            )
            result = cursor.fetchone()

            if result:
                current_status, updated_time = result
                # Calculate time difference
                time_diff = datetime.now() - updated_time

                if time_diff > timedelta(days=1):
                    # Update status to 'failed' if updated_time is older than 1 day
                    cursor.execute(
                        'UPDATE mkpipe_manifest SET status = %s, updated_time = CURRENT_TIMESTAMP WHERE table_name = %s',
                        ('failed', name),
                    )
                    conn.commit()
                    return 'failed'
                else:
                    # Return the current status if updated_time is within 1 day
                    return current_status
            else:
                # Return None if the table_name is not found
                return None


@retry_on_failure()
def get_last_point(name):
    config = load_config()
    connection_params = config['settings']['backend']
    db_type = config['settings']['backend']['database_type']
    db_connector = get_db_connector(db_type)

    with db_connector(connection_params) as conn:
        with conn.cursor() as cursor:
            # Retrieve last_point
            cursor.execute(
                'SELECT last_point FROM mkpipe_manifest WHERE table_name = %s', (name,)
            )
            result = cursor.fetchone()
            return result[0] if result else None


@retry_on_failure()
def manifest_table_update(
    name,
    value,
    value_type,
    status='completed',
    replication_method='full',
    error_message=None,
):
    """
    Update or insert the last point value, value type, status, and error message for a specified table.
    In case of failure, log the error message.
    """
    config = load_config()
    connection_params = config['settings']['backend']
    db_type = config['settings']['backend']['database_type']
    db_connector = get_db_connector(db_type)

    with db_connector(connection_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                'SELECT table_name FROM mkpipe_manifest WHERE table_name = %s', (name,)
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

                # Set updated_time to current timestamp
                update_fields.append('updated_time = CURRENT_TIMESTAMP')

                update_values.append(name)  # Last value is the table name

                # Construct the update SQL statement
                update_sql = f"""
                    UPDATE mkpipe_manifest
                    SET {', '.join(update_fields)}
                    WHERE table_name = %s
                """
                cursor.execute(update_sql, tuple(update_values))
            else:
                # Insert new entry with updated_time set to default (CURRENT_TIMESTAMP)
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
