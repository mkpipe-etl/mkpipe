import clickhouse_connect
import time
from datetime import datetime, timedelta
from functools import wraps
from ..utils import Logger


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


class ConnectorClickhouse:
    def __init__(self, connection_params):
        self.connection_params = connection_params

    def connect(self):
        return clickhouse_connect.get_client(
            host=self.connection_params['host'],
            port=self.connection_params.get('port', 8123),
            username=self.connection_params['user'],
            password=self.connection_params['password'],
            database=self.connection_params['database'],
        )

    @retry_on_failure()
    def get_table_status(self, name):
        client = self.connect()
        client.command("""
            CREATE TABLE IF NOT EXISTS mkpipe_manifest (
                table_name String,
                last_point Nullable(String),
                type Nullable(String),
                replication_method Nullable(String),
                status Nullable(String),
                error_message Nullable(String),
                updated_time DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY table_name;
        """)

        query_result = client.query(
            f"SELECT status, updated_time FROM mkpipe_manifest WHERE table_name = '{name}'"
        )

        result = query_result.first_row if query_result.row_count != 0 else None
        if result:
            current_status, updated_time = result
            time_diff = datetime.now() - updated_time
            if time_diff > timedelta(days=1):
                client.command(
                    f"""
                    ALTER TABLE mkpipe_manifest UPDATE status = 'failed', updated_time = now()
                    WHERE table_name = '{name}'
                """
                )
                return 'failed'
            else:
                return current_status
        else:
            return None

    @retry_on_failure()
    def get_last_point(self, name):
        client = self.connect()
        query_result = client.query(
            f"SELECT last_point FROM mkpipe_manifest WHERE table_name = '{name}'"
        )

        result = query_result.first_row if query_result.row_count != 0 else None
        return result[0]

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
        client = self.connect()

        query_result = client.query(
            f"SELECT table_name FROM mkpipe_manifest WHERE table_name = '{name}'"
        )

        result = query_result.first_row if query_result.row_count != 0 else None
        if result:
            update_parts = []

            if value is not None:
                update_parts.append(f"last_point = '{value}'")
            if value_type is not None:
                update_parts.append(f"type = '{value_type}'")

            update_parts.append(f"status = '{status}'")
            update_parts.append(f"replication_method = '{replication_method}'")

            if error_message is not None:
                update_parts.append(f"error_message = '{error_message}'")

            update_parts.append('updated_time = now()')

            update_sql = f"""
                ALTER TABLE mkpipe_manifest UPDATE {', '.join(update_parts)} WHERE table_name = '{name}'
            """
            client.command(update_sql)
        else:
            client.command(
                f"""
                INSERT INTO mkpipe_manifest (
                    table_name, last_point, type, status,
                    replication_method, error_message, updated_time
                ) VALUES (
                    {format_clickhouse_value(name)},
                    {format_clickhouse_value(value)},
                    {format_clickhouse_value(value_type)},
                    {format_clickhouse_value(status)},
                    {format_clickhouse_value(replication_method)},
                    {format_clickhouse_value(error_message)},
                    now()
                )
                """
            )


def format_clickhouse_value(val):
    if val is None:
        return 'NULL'
    elif isinstance(val, str):
        return f"'{val}'"
    elif isinstance(val, (int, float)):
        return str(val)
    else:
        raise ValueError(f'Unsupported value type: {type(val)}')
