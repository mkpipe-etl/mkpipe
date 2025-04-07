import clickhouse_connect
import time
from datetime import datetime, timedelta
from functools import wraps
from ..utils import Logger

logger = Logger(__file__)


def retry_on_failure(max_attempts=5, delay=1):
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
        """
        Check the status of a table from the mkpipe_manifest. If the updated_time is older than 1 day,
        update the status to 'failed' and return 'failed'. Otherwise, return the current status.
        If the table does not exist, create it first.
        """
        client = self.connect()
        client.command("""
            CREATE TABLE IF NOT EXISTS mkpipe_manifest (
                table_name String,
                last_point String,
                type String,
                replication_method Enum('incremental' = 1, 'full' = 2),
                status Enum('completed' = 1, 'failed' = 2, 'extracting' = 3, 'loading' = 4, 'extracted' = 5, 'loaded' = 6),
                error_message String,
                updated_time DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY table_name;
        """)

        result = client.query(
            'SELECT status, updated_time FROM mkpipe_manifest WHERE table_name = %(name)s',
            parameters={'name': name},
        ).first_row

        if result:
            current_status, updated_time = result
            time_diff = datetime.now() - updated_time
            if time_diff > timedelta(days=1):
                client.command(
                    """
                    ALTER TABLE mkpipe_manifest UPDATE status = 'failed', updated_time = now()
                    WHERE table_name = %(name)s
                """,
                    parameters={'name': name},
                )
                return 'failed'
            else:
                return current_status
        else:
            return None

    @retry_on_failure()
    def get_last_point(self, name):
        client = self.connect()
        result = client.query(
            'SELECT last_point FROM mkpipe_manifest WHERE table_name = %(name)s',
            parameters={'name': name},
        ).first_row
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
        client = self.connect()

        result = client.query(
            'SELECT table_name FROM mkpipe_manifest WHERE table_name = %(name)s',
            parameters={'name': name},
        ).first_row

        if result:
            update_parts = []
            update_params = {}

            if value is not None:
                update_parts.append('last_point = %(last_point)s')
                update_params['last_point'] = value
            if value_type is not None:
                update_parts.append('type = %(type)s')
                update_params['type'] = value_type

            update_parts.append('status = %(status)s')
            update_params['status'] = status

            update_parts.append('replication_method = %(replication_method)s')
            update_params['replication_method'] = replication_method

            if error_message is not None:
                update_parts.append('error_message = %(error_message)s')
                update_params['error_message'] = error_message

            update_parts.append('updated_time = now()')
            update_params['name'] = name

            update_sql = f"""
                ALTER TABLE mkpipe_manifest UPDATE {', '.join(update_parts)} WHERE table_name = %(name)s
            """
            client.command(update_sql, parameters=update_params)
        else:
            client.command(
                """
                INSERT INTO mkpipe_manifest (
                    table_name, last_point, type, status,
                    replication_method, error_message, updated_time
                ) VALUES (
                    %(name)s, %(last_point)s, %(type)s, %(status)s,
                    %(replication_method)s, %(error_message)s, now()
                )
            """,
                parameters={
                    'name': name,
                    'last_point': value or '',
                    'type': value_type or '',
                    'status': status,
                    'replication_method': replication_method,
                    'error_message': error_message or '',
                },
            )
