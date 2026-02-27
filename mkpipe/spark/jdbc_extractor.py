import os
from urllib.parse import quote_plus
from typing import Dict, Optional

from .base import BaseExtractor
from ..exceptions import ConfigError
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
        iterate_column = table.iterate_column
        iterate_column_type = table.iterate_column_type
        custom_query = self._resolve_custom_query(table)

        if not iterate_column:
            raise ConfigError(
                f"Table '{name}': incremental replication requires 'iterate_column'"
            )

        partitions_count = table.partitions_count
        partitions_column_raw = table.partitions_column or iterate_column
        partitions_column = self._normalize_partitions_column(partitions_column_raw)
        p_col_name = partitions_column_raw.split(' as ')[-1].strip()
        fetchsize = table.fetchsize
        jdbc_url = self.build_jdbc_url()

        # --- Step 1: Get iterate_column bounds for the new data window ---
        iterate_col_normalized = self._normalize_partitions_column(iterate_column)

        if last_point:
            bounds_query = (
                f"(SELECT min({iterate_col_normalized}) AS min_val, "
                f"max({iterate_col_normalized}) AS max_val "
                f"FROM {name} WHERE {iterate_col_normalized} >= '{last_point}') q"
            )
            write_mode = 'append'
        else:
            bounds_query = (
                f"(SELECT min({iterate_col_normalized}) AS min_val, "
                f"max({iterate_col_normalized}) AS max_val "
                f"FROM {name}) q"
            )
            write_mode = 'overwrite'

        df_bounds = self._build_reader(spark, jdbc_url, bounds_query)
        row = df_bounds.first()

        if not row or row[0] is None:
            if not last_point:
                return self._extract_full(table, spark)
            logger.info({'table': table.target_name, 'status': 'no_new_data'})
            return ExtractResult(df=None, write_mode=write_mode)

        min_val, max_val = row[0], row[1]

        if iterate_column_type == 'int':
            min_iterate = int(min_val)
            max_iterate = int(max_val)
        elif iterate_column_type == 'datetime':
            min_iterate = min_val.strftime('%Y-%m-%d %H:%M:%S.%f')
            max_iterate = max_val.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            raise ConfigError(
                f"Table '{name}': unsupported iterate_column_type '{iterate_column_type}'. "
                f"Supported: 'int', 'datetime'"
            )

        # --- Step 2: Build filter clause using iterate_column ---
        if iterate_column_type == 'int':
            filter_clause = (
                f"WHERE {iterate_col_normalized} >= {min_iterate} "
                f"AND {iterate_col_normalized} <= {max_iterate}"
            )
        else:
            filter_clause = (
                f"WHERE {iterate_col_normalized} >= '{min_iterate}' "
                f"AND {iterate_col_normalized} <= '{max_iterate}'"
            )

        if custom_query:
            updated_query = custom_query.replace('{query_filter}', f' {filter_clause} ')
        else:
            updated_query = f'(SELECT * FROM {name} {filter_clause}) q'

        # --- Step 3: Resolve partition bounds (may differ from iterate bounds) ---
        if partitions_count and partitions_column != iterate_col_normalized:
            p_bounds_query = (
                f"(SELECT min({partitions_column}) AS p_min, "
                f"max({partitions_column}) AS p_max "
                f"FROM {name} {filter_clause}) q"
            )
            p_row = self._build_reader(spark, jdbc_url, p_bounds_query).first()
            p_lower = p_row[0] if p_row and p_row[0] is not None else min_iterate
            p_upper = p_row[1] if p_row and p_row[1] is not None else max_iterate
        else:
            p_lower = min_iterate
            p_upper = max_iterate

        df = self._build_reader(
            spark, jdbc_url, updated_query,
            fetchsize=fetchsize,
            partitions=partitions_count,
            partition_column=p_col_name,
            lower_bound=p_lower,
            upper_bound=p_upper,
        )

        return ExtractResult(
            df=df,
            write_mode=write_mode,
            last_point_value=str(max_iterate),
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
