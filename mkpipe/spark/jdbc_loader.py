import gc
from datetime import datetime
from typing import Dict
from urllib.parse import quote_plus

from .base import BaseLoader
from .columns import add_etl_columns
from ..exceptions import LoadError
from ..models import ConnectionConfig, ExtractResult, TableConfig
from ..utils import get_logger

logger = get_logger(__name__)


class JdbcLoader(BaseLoader):
    driver_name: str = ''
    driver_jdbc: str = ''

    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.host = connection.host
        self.port = connection.port
        self.username = connection.user
        self.password = quote_plus(str(connection.password or ''))
        self.database = connection.database
        self.schema = connection.schema
        self.warehouse = connection.warehouse
        self.private_key_file = connection.private_key_file
        self.private_key_file_pwd = connection.private_key_file_pwd

    def build_jdbc_url(self) -> str:
        return (
            f'jdbc:{self.driver_name}://{self.host}:{self.port}/{self.database}'
            f'?user={self.username}&password={self.password}'
        )

    def _jdbc_options(self) -> Dict[str, str]:
        """Override in subclass for extra JDBC properties (RSA key, SSL, OAuth, etc.)"""
        return {}

    def _write_df(self, df, write_mode: str, table_name: str, batchsize: int):
        jdbc_url = self.build_jdbc_url()
        writer = (
            df.write.format('jdbc')
            .mode(write_mode)
            .option('url', jdbc_url)
            .option('dbtable', table_name)
            .option('driver', self.driver_jdbc)
            .option('batchsize', batchsize)
        )
        for k, v in self._jdbc_options().items():
            writer = writer.option(k, v)
        writer.save()
        df.unpersist()
        gc.collect()

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        batchsize = table.batchsize
        write_mode = data.write_mode
        df = data.df

        if df is None:
            logger.info({
                'table': target_name,
                'status': 'skipped',
                'reason': 'no data to load',
            })
            return

        df = add_etl_columns(df, datetime.now(), dedup_columns=table.dedup_columns)

        if table.write_partitions:
            df = df.coalesce(table.write_partitions)

        logger.info({
            'table': target_name,
            'status': 'loading',
            'write_mode': write_mode,
        })

        try:
            self._write_df(df, write_mode, target_name, batchsize)
        except Exception as e:
            raise LoadError(f"Failed to write '{target_name}': {e}") from e

        logger.info({
            'table': target_name,
            'status': 'loaded',
        })
