import os
import psycopg2
import sqlite3
import time
from datetime import datetime, timedelta
from functools import wraps
from .register_db import register_db_connector, get_db_connector
from ..config import load_config, ROOT_DIR
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


@register_db_connector('sqlite')
def connect_sqlite(connection_params):
    """
    Establish a connection to a SQLite database using provided connection parameters.

    Parameters:
        connection_params (dict): Dictionary containing the database file path.

    Returns:
        connection: A connection object for the SQLite database.
    """
    database = str(ROOT_DIR / connection_params['database'])

    # Ensure the directory for the database exists
    db_dir = os.path.dirname(database)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    return sqlite3.connect(database)


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
    db_type = connection_params['database_type']
    db_connector = get_db_connector(db_type)

    with db_connector(connection_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS mkpipe_manifest (
                    table_name TEXT NOT NULL,
                    last_point TEXT,
                    type TEXT,
                    replication_method TEXT,
                    status TEXT,
                    error_message TEXT,
                    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_table_name UNIQUE (table_name)
                );
            """)
            conn.commit()

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
                        """
                        UPDATE mkpipe_manifest 
                        SET status = %s, updated_time = CURRENT_TIMESTAMP 
                        WHERE table_name = %s
                        """,
                        ('failed', name),
                    )
                    conn.commit()
                    return 'failed'
                return current_status
            else:
                return None


@retry_on_failure()
def get_last_point(name):
    """
    Retrieve the last_point value for the given table from mkpipe_manifest.
    """
    config = load_config()
    connection_params = config['settings']['backend']
    db_type = connection_params['database_type']
    db_connector = get_db_connector(db_type)

    with db_connector(connection_params) as conn:
        with conn.cursor() as cursor:
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
    db_type = connection_params['database_type']
    db_connector = get_db_connector(db_type)

    with db_connector(connection_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                'SELECT table_name FROM mkpipe_manifest WHERE table_name = %s', (name,)
            )
            if cursor.fetchone():
                update_fields = []
                update_values = []

                if value is not None:
                    update_fields.append('last_point = %s')
                    update_values.append(value)

                if value_type is not None:
                    update_fields.append('type = %s')
                    update_values.append(value_type)

                update_fields.extend(
                    [
                        'status = %s',
                        'replication_method = %s',
                        'updated_time = CURRENT_TIMESTAMP',
                    ]
                )
                update_values.extend([status, replication_method])

                if error_message is not None:
                    update_fields.append('error_message = %s')
                    update_values.append(error_message)

                update_values.append(name)
                update_sql = f"""
                    UPDATE mkpipe_manifest 
                    SET {', '.join(update_fields)} 
                    WHERE table_name = %s
                """
                cursor.execute(update_sql, tuple(update_values))
            else:
                cursor.execute(
                    """
                    INSERT INTO mkpipe_manifest 
                    (table_name, last_point, type, status, replication_method, error_message, updated_time)
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
