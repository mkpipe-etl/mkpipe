import os
import datetime
from urllib.parse import quote_plus

from .session import create_spark_session
from ..config import load_config
from ..utils import log_container, Logger, PipeSettings
from ..functions_db import get_db_connector


class BaseExtractor:
    def __init__(self, config, settings, driver_name, driver_jdbc):
        if isinstance(settings, dict):
            self.settings = PipeSettings(**settings)
        else:
            self.settings = settings
        self.connection_params = config['connection_params']
        self.table = config['table']
        self.pass_on_error = config.get('pass_on_error', None)
        self.host = self.connection_params.get('host', None)
        self.port = self.connection_params.get('port', None)
        self.username = self.connection_params.get('user', None)
        self.password = self.build_passord()
        self.database = self.connection_params.get('database', None)
        self.schema = self.connection_params.get('schema', None)
        self.warehouse = self.connection_params.get('warehouse', None)
        self.private_key_file = self.connection_params.get('private_key_file', None)
        self.private_key_file_pwd = self.connection_params.get('private_key_file_pwd', None)

        self.driver_name = driver_name
        self.driver_jdbc = driver_jdbc
        self.settings.driver_name = self.driver_name
        self.jdbc_url = self.build_jdbc_url()

        config = load_config()
        connection_params = config['settings']['backend']
        db_type = connection_params['variant']
        self.backend = get_db_connector(db_type)(connection_params)

    def build_jdbc_url(self):
        return f'jdbc:{self.driver_name}://{self.host}:{self.port}/{self.database}?user={self.username}&password={self.password}'

    def build_passord(self):
        return quote_plus(str(self.connection_params.get('password', None)))

    def normalize_partitions_column(self, col: str):
        return col.split(' as ')[0].strip()
        # return  '"' + partitions_column_.split(' as ')[0].strip() + '"'

    def extract_incremental(self, t):
        logger = Logger(__file__)
        spark = create_spark_session(self.settings)

        try:
            name = t['name']
            target_name = t['target_name']
            iterate_column_type = t['iterate_column_type']
            custom_query = t.get('custom_query', None)
            custom_query_file = t.get('custom_query_file', None)
            if custom_query_file:
                custom_query_file_path = os.path.abspath(
                    os.path.join(self.settings.ROOT_DIR, 'sql', custom_query_file)
                )
                with open(custom_query_file_path, 'r') as f:
                    custom_query = f.read()

            custom_partitions_count = t.get('partitions_count', self.settings.partitions_count)
            partitions_column_ = t.get('partitions_column')
            fetchsize = t.get('fetchsize', 100_000)

            partitions_column = self.normalize_partitions_column(partitions_column_)
            p_col_name = partitions_column_.split(' as ')[-1].strip()

            last_point = self.backend.get_last_point(target_name)
            # write_mode = 'overwrite'
            write_mode = 'append'
            iterate_query = f"""
                select 
                    min({partitions_column}) as min_val, 
                    max({partitions_column}) as max_val, 
                    count(*) as record_count
                from {name} 
            """
            if last_point:
                iterate_query = (
                    iterate_query + f""" {name} where {partitions_column} > '{last_point}' """
                )

            df_iterate_list = (
                spark.read.format('jdbc')
                .option('url', self.jdbc_url)
                .option('query', iterate_query)
                .option('driver', self.driver_jdbc)
                .load()
            )

            row = df_iterate_list.first()
            min_val, max_val, record_count = row[0], row[1], row[2]

            # 3. Exit if no new data
            if not row or record_count == 0:
                if not last_point:
                    # Empty table, need schema fetc
                    return self.extract_full(t)
                else:
                    # Not empt, but no new data, all fetched before
                    data = {
                        'write_mode': write_mode,
                        'df': None,
                    }
                    return data

            if iterate_column_type == 'int':
                min_filter = int(min_val)
                max_filter = int(max_val)
                if custom_query:
                    updated_query = custom_query.replace(
                        '{query_filter}',
                        f""" where {partitions_column} >= {min_filter} and {partitions_column} <= {max_filter} """,
                    )
                else:
                    updated_query = f'(SELECT * from {name} where {partitions_column} >= {min_filter} and {partitions_column} <= {max_filter}) q'
            elif iterate_column_type == 'datetime':
                min_filter = min_val.strftime('%Y-%m-%d %H:%M:%S.%f')
                max_filter = max_val.strftime('%Y-%m-%d %H:%M:%S.%f')
                if custom_query:
                    updated_query = custom_query.replace(
                        '{query_filter}',
                        f""" where {partitions_column} >= '{min_filter}' and {partitions_column} <= '{max_filter}' """,
                    )
                else:
                    updated_query = f"""(SELECT * from {name} where  {partitions_column} >= '{min_filter}' and {partitions_column} <= '{max_filter}') q"""
            else:
                raise ValueError(f'Unsupported iterate_column_type: {iterate_column_type}')

            df = (
                spark.read.format('jdbc')
                .option('url', self.jdbc_url)
                .option('dbtable', updated_query)
                .option('driver', self.driver_jdbc)
                .option('numPartitions', custom_partitions_count)
                .option('partitionColumn', p_col_name)
                .option('lowerBound', min_filter)
                .option('upperBound', max_filter)
                .option('fetchsize', fetchsize)
                .load()
            )

            last_point_value = str(max_filter)
            data = {'write_mode': write_mode, 'last_point_value': last_point_value, 'df': df}
            return data
        except Exception as e:
            raise e

    def extract_full(self, t):
        logger = Logger(__file__)
        spark = create_spark_session(self.settings)
        try:
            name = t['name']
            target_name = t['target_name']
            iterate_column_type = t.get('iterate_column_type', None)
            message = dict(table_name=target_name, status='extracting')
            logger.info(message)
            custom_partitions_count = t.get('partitions_count', self.settings.partitions_count)
            fetchsize = t.get('fetchsize', 100_000)
            partitions_column_ = t.get('partitions_column', None)

            custom_query = t.get('custom_query', None)
            custom_query_file = t.get('custom_query_file', None)
            if custom_query_file:
                custom_query_file_path = os.path.abspath(
                    os.path.join(self.settings.ROOT_DIR, 'sql', custom_query_file)
                )
                with open(custom_query_file_path, 'r') as f:
                    custom_query = f.read()

            write_mode = 'overwrite'

            if not custom_query:
                updated_query = f'(SELECT * from {name}) q'
            else:
                updated_query = custom_query.replace(
                    '{query_filter}',
                    ' where 1=1 ',
                )

            if partitions_column_:
                partitions_column = partitions_column_.split(' as ')[0]
                p_col_name = partitions_column_.split(' as ')[-1]

                iterate_query = f"""
                    select 
                        min({partitions_column}) as min_val, 
                        max({partitions_column}) as max_val, 
                        count(*) as record_count
                    from {name} 
                """

                df_iterate_list = (
                    spark.read.format('jdbc')
                    .option('url', self.jdbc_url)
                    .option('query', iterate_query)
                    .option('driver', self.driver_jdbc)
                    .load()
                )

                row = df_iterate_list.first()
                min_val, max_val, record_count = row[0], row[1], row[2]

                if iterate_column_type == 'int':
                    min_filter = int(min_val)
                    max_filter = int(max_val)
                elif iterate_column_type == 'datetime':
                    min_filter = min_val.strftime('%Y-%m-%d %H:%M:%S.%f')
                    max_filter = max_val.strftime('%Y-%m-%d %H:%M:%S.%f')

                if not row or record_count == 0:
                    # empty df, we need schema
                    df = (
                        spark.read.format('jdbc')
                        .option('url', self.jdbc_url)
                        .option('dbtable', updated_query)
                        .option('driver', self.driver_jdbc)
                        .option('fetchsize', fetchsize)
                        .load()
                    )
                else:
                    # which means table not empt
                    df = (
                        spark.read.format('jdbc')
                        .option('url', self.jdbc_url)
                        .option('dbtable', updated_query)
                        .option('driver', self.driver_jdbc)
                        .option('numPartitions', custom_partitions_count)
                        .option('partitionColumn', p_col_name)
                        .option('lowerBound', min_filter)
                        .option('upperBound', max_filter)
                        .option('fetchsize', fetchsize)
                        .load()
                    )

            else:
                df = (
                    spark.read.format('jdbc')
                    .option('url', self.jdbc_url)
                    .option('dbtable', updated_query)
                    .option('driver', self.driver_jdbc)
                    .option('fetchsize', fetchsize)
                    .load()
                )

            data = {
                'write_mode': write_mode,
                'df': df,
            }
            message = dict(
                table_name=target_name,
                status='extracted',
                meta_data=data,
            )
            logger.info(message)
            return data
        except Exception as e:
            raise e

    @log_container(__file__)
    def extract(self):
        extract_start_time = datetime.datetime.now()
        logger = Logger(__file__)
        logger.info({'message': 'Extracting data ...'})
        logger.warning(
            'Performing full extract of a table or without partition column with incremental table. Can cause OOM errors for large tables.'
        )
        t = self.table
        try:
            target_name = t['target_name']
            replication_method = t.get('replication_method', None)
            if self.backend.get_table_status(target_name) in ['extracting', 'loading']:
                logger.info({'message': f'Skipping {target_name}, already in progress...'})
                data = {
                    'status': 'completed',
                    'df': None,
                }
                return data

            self.backend.manifest_table_update(
                name=target_name,
                value=None,  # Last point remains unchanged
                value_type=None,  # Type remains unchanged
                status='extracting',  # ('completed', 'failed', 'extracting', 'loading')
                replication_method=replication_method,  # ('incremental', 'full')
                error_message='',
            )
            if replication_method == 'incremental':
                return self.extract_incremental(t)
            else:
                return self.extract_full(t)

        except Exception as e:
            message = dict(
                table_name=target_name,
                status='failed',
                type='pipeline',
                error_message=str(e),
                etl_start_time=str(extract_start_time),
            )
            self.backend.manifest_table_update(
                target_name,
                None,
                None,
                status='failed',
                replication_method=replication_method,
                error_message=str(e),
            )
            if self.pass_on_error:
                logger.warning(message)
                return None
            else:
                raise Exception(message) from e
