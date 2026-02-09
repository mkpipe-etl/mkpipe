import os
from urllib.parse import quote_plus
from typing import Dict, Optional

from .base import BaseExtractor
from ..models import ConnectionConfig, ExtractResult, TableConfig
from ..utils import get_logger

logger = get_logger(__name__)


class JdbcExtractor(BaseExtractor):
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

    def _build_reader(self, spark, jdbc_url: str, dbtable: str, fetchsize: int = 0, partitions: int = 0, partition_column: str = None, lower_bound=None, upper_bound=None):
        reader = (
            spark.read.format('jdbc')
            .option('url', jdbc_url)
            .option('dbtable', dbtable)
            .option('driver', self.driver_jdbc)
        )
        if fetchsize:
            reader = reader.option('fetchsize', fetchsize)
        if partitions and partition_column:
            reader = (reader
                .option('numPartitions', partitions)
                .option('partitionColumn', partition_column)
                .option('lowerBound', lower_bound)
                .option('upperBound', upper_bound)
            )
        for k, v in self._jdbc_options().items():
            reader = reader.option(k, v)
        return reader.load()

    def _resolve_custom_query(self, table: TableConfig, root_dir: Optional[str] = None) -> Optional[str]:
        if table.custom_query:
            return table.custom_query
        if table.custom_query_file:
            base = root_dir or os.getcwd()
            path = os.path.abspath(os.path.join(base, 'sql', table.custom_query_file))
            with open(path, 'r') as f:
                return f.read()
        return None

    def _normalize_partitions_column(self, col: str) -> str:
        return col.split(' as ')[0].strip()

    def _extract_incremental(self, table: TableConfig, spark, last_point: Optional[str]) -> ExtractResult:
        name = table.name
        iterate_column_type = table.iterate_column_type
        custom_query = self._resolve_custom_query(table)

        partitions_count = table.partitions_count
        partitions_column_raw = table.partitions_column or table.iterate_column
        if not partitions_column_raw:
            raise ValueError(
                f"Table '{name}': incremental replication requires "
                f"'iterate_column' or 'partitions_column'"
            )

        partitions_column = self._normalize_partitions_column(partitions_column_raw)
        p_col_name = partitions_column_raw.split(' as ')[-1].strip()
        fetchsize = table.fetchsize
        jdbc_url = self.build_jdbc_url()

        iterate_query = (
            f"(SELECT min({partitions_column}) AS min_val, "
            f"max({partitions_column}) AS max_val, "
            f"count(*) AS record_count FROM "
        )

        if last_point:
            iterate_query += f"{name} WHERE {partitions_column} > '{last_point}') q"
            write_mode = 'append'
        else:
            iterate_query += f"{name}) q"
            write_mode = 'overwrite'

        df_bounds = self._build_reader(spark, jdbc_url, iterate_query)

        row = df_bounds.first()
        min_val, max_val, record_count = row[0], row[1], row[2]

        if not row or record_count == 0:
            if not last_point:
                return self._extract_full(table, spark)
            return ExtractResult(df=None, write_mode=write_mode)

        if iterate_column_type == 'int':
            min_filter = int(min_val)
            max_filter = int(max_val)
            filter_clause = (
                f"WHERE {partitions_column} >= {min_filter} "
                f"AND {partitions_column} <= {max_filter}"
            )
        elif iterate_column_type == 'datetime':
            min_filter = min_val.strftime('%Y-%m-%d %H:%M:%S.%f')
            max_filter = max_val.strftime('%Y-%m-%d %H:%M:%S.%f')
            filter_clause = (
                f"WHERE {partitions_column} >= '{min_filter}' "
                f"AND {partitions_column} <= '{max_filter}'"
            )
        else:
            raise ValueError(f"Unsupported iterate_column_type: {iterate_column_type}")

        if custom_query:
            updated_query = custom_query.replace('{query_filter}', f' {filter_clause} ')
        else:
            updated_query = f'(SELECT * FROM {name} {filter_clause}) q'

        df = self._build_reader(
            spark, jdbc_url, updated_query,
            fetchsize=fetchsize,
            partitions=partitions_count,
            partition_column=p_col_name,
            lower_bound=min_filter,
            upper_bound=max_filter,
        )

        return ExtractResult(
            df=df,
            write_mode=write_mode,
            last_point_value=str(max_filter),
        )

    def _extract_full(self, table: TableConfig, spark) -> ExtractResult:
        name = table.name
        fetchsize = table.fetchsize
        custom_query = self._resolve_custom_query(table)
        jdbc_url = self.build_jdbc_url()

        if custom_query:
            updated_query = custom_query.replace('{query_filter}', ' WHERE 1=1 ')
        else:
            updated_query = f'(SELECT * FROM {name}) q'

        df = self._build_reader(spark, jdbc_url, updated_query, fetchsize=fetchsize)

        return ExtractResult(df=df, write_mode='overwrite')

    def extract(self, table: TableConfig, spark, last_point: Optional[str] = None) -> ExtractResult:
        logger.info({
            'table': table.target_name,
            'status': 'extracting',
            'replication_method': table.replication_method.value,
        })

        if table.replication_method.value == 'incremental':
            result = self._extract_incremental(table, spark, last_point)
        else:
            result = self._extract_full(table, spark)

        logger.info({
            'table': table.target_name,
            'status': 'extracted',
            'write_mode': result.write_mode,
        })
        return result
