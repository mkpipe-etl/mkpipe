import time
from urllib.parse import quote_plus
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
import gc

from .parquet_functions import remove_partitioned_parquet
from .register_file_parser import get_parser
from ..config import load_config
from ..utils import log_container, Logger, PipeSettings
from ..functions_db import get_db_connector


class BaseLoader:
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

    def add_custom_columns(self, df, elt_start_time):
        if 'etl_time' in df.columns:
            df = df.drop('etl_time')
        df = df.withColumn('etl_time', F.lit(elt_start_time).cast(TimestampType()))
        return df

    def write_df(self, df, write_mode, table_name, batchsize):
        (
            df.write.format('jdbc')
            .mode(write_mode)  # Use write_mode for the first iteration, 'append' for others
            .option('url', self.jdbc_url)
            .option('dbtable', table_name)
            .option('driver', self.driver_jdbc)
            .option('batchsize', batchsize)
            .save()
        )
        df.unpersist()
        gc.collect()

        """
        for index in range(df.rdd.getNumPartitions()):
            # Filter the DataFrame by partition index
            partition_df = df.filter(F.spark_partition_id() == index)

            # Determine the correct write mode (use write_mode for the first iteration, 'append' for others)
            current_write_mode = write_mode if index == 0 else 'append'

            (
                partition_df.write.format('jdbc')
                .mode(
                    current_write_mode
                )  # Use write_mode for the first iteration, 'append' for others
                .option('url', self.jdbc_url)
                .option('dbtable', name)
                .option('driver', self.driver_jdbc)
                .option('batchsize', batchsize)
                .save()
            )
        """
        return

    @log_container(__file__)
    def load(self, data, elt_start_time):
        try:
            logger = Logger(__file__)
            start_time = time.time()
            name = data['table_name']

            write_mode = data.get('write_mode', None)
            file_type = data.get('file_type', None)
            last_point_value = data.get('last_point_value', None)
            iterate_column_type = data.get('iterate_column_type', None)
            replication_method = data.get('replication_method', 'full')
            batchsize = data.get('fetchsize', 100_000)
            pass_on_error = data.get('pass_on_error', None)

            if not file_type:
                'means that the data fetched before no new data'
                self.backend.manifest_table_update(
                    name=name,
                    value=None,  # Last point remains unchanged
                    value_type=None,  # Type remains unchanged
                    status='completed',  # ('completed', 'failed', 'extracting', 'loading')
                    replication_method=replication_method,  # ('incremental', 'full')
                    error_message='',
                )
                return

            self.backend.manifest_table_update(
                name=name,
                value=None,  # Last point remains unchanged
                value_type=None,  # Type remains unchanged
                status='loading',  # ('completed', 'failed', 'extracting', 'loading')
                replication_method=replication_method,  # ('incremental', 'full')
                error_message='',
            )

            df = get_parser(file_type)(data, self.settings)
            df = self.add_custom_columns(df, elt_start_time)
            message = dict(
                table_name=name,
                status='loading',
                total_partition_count=df.rdd.getNumPartitions(),
            )
            logger.info(message)

            self.write_df(df=df, write_mode=write_mode, table_name=name, batchsize=batchsize)

            # Update last point in the mkpipe_manifest table if applicable
            self.backend.manifest_table_update(
                name=name,
                value=last_point_value,
                value_type=iterate_column_type,
                status='completed',
                replication_method=replication_method,
                error_message='',
            )

            message = dict(table_name=name, status=write_mode)
            logger.info(message)

            # remove the parquet to reduce the storage
            remove_partitioned_parquet(data['path'])

            run_time = time.time() - start_time
            message = dict(table_name=name, status='success', run_time=run_time)
            logger.info(message)

        except Exception as e:
            # Log the error message and update the mkpipe_manifest with the error details
            message = dict(
                table_name=name,
                status='failed',
                type='loading',
                error_message=str(e),
                etl_start_time=str(elt_start_time),
            )

            self.backend.manifest_table_update(
                name=name,
                value=None,  # Last point remains unchanged
                value_type=None,  # Type remains unchanged
                status='failed',
                replication_method=replication_method,
                error_message=str(e),
            )

            if pass_on_error:
                logger.warning(message)
                return
            else:
                logger.error(message)
                raise Exception(message) from e
        return
