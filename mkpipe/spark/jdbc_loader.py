import gc
from datetime import datetime
from typing import Dict, List
from urllib.parse import quote_plus

from .base import BaseLoader
from .columns import add_etl_columns
from ..exceptions import ConfigError, LoadError
from ..models import ConnectionConfig, ExtractResult, TableConfig, WriteStrategy
from ..strategy import resolve_write_strategy
from ..utils import get_logger

logger = get_logger(__name__)


class JdbcLoader(BaseLoader):
    driver_name: str = ''
    driver_jdbc: str = ''
    _dialect: str = 'ansi'

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

    # ── SQL execution via Spark JDBC ──────────────────────────────────

    def _execute_sql(self, sql: str, spark) -> None:
        jdbc_url = self.build_jdbc_url()
        props = {'driver': self.driver_jdbc}
        props.update(self._jdbc_options())
        conn = spark._jvm.java.sql.DriverManager.getConnection(
            jdbc_url,
            self.username,
            quote_plus(str(self.connection.password or '')),
        )
        try:
            stmt = conn.createStatement()
            try:
                stmt.execute(sql)
            finally:
                stmt.close()
        finally:
            conn.close()

    # ── Dialect-specific MERGE / UPSERT SQL builders ──────────────────

    def _build_upsert_sql(
        self,
        temp_table: str,
        target_table: str,
        write_key: List[str],
        columns: List[str],
    ) -> str:
        non_key_cols = [c for c in columns if c not in write_key]
        return self._build_merge_sql(temp_table, target_table, write_key, columns, non_key_cols)

    def _build_merge_sql(
        self,
        temp_table: str,
        target_table: str,
        write_key: List[str],
        columns: List[str],
        update_columns: List[str],
    ) -> str:
        dialect = self._dialect

        join_cond = ' AND '.join(f't."{k}" = s."{k}"' for k in write_key)
        insert_cols = ', '.join(f'"{c}"' for c in columns)
        insert_vals = ', '.join(f's."{c}"' for c in columns)
        update_set = ', '.join(f'"{c}" = s."{c}"' for c in update_columns)

        if dialect in ('postgres', 'postgresql', 'redshift', 'timescaledb', 'sqlite'):
            conflict_cols = ', '.join(f'"{c}"' for c in write_key)
            update_set_pg = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in update_columns)
            return (
                f'INSERT INTO {target_table} ({insert_cols}) '
                f'SELECT {insert_cols} FROM {temp_table} '
                f'ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set_pg}'
            )

        if dialect in ('mysql', 'mariadb'):
            update_set_my = ', '.join(f'`{c}` = VALUES(`{c}`)' for c in update_columns)
            insert_cols_my = ', '.join(f'`{c}`' for c in columns)
            return (
                f'INSERT INTO {target_table} ({insert_cols_my}) '
                f'SELECT {insert_cols_my} FROM {temp_table} '
                f'ON DUPLICATE KEY UPDATE {update_set_my}'
            )

        if dialect == 'sqlserver':
            return (
                f'MERGE {target_table} AS t '
                f'USING {temp_table} AS s ON {join_cond} '
                f'WHEN MATCHED THEN UPDATE SET {update_set} '
                f'WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});'
            )

        if dialect in ('oracle', 'oracledb'):
            return (
                f'MERGE INTO {target_table} t '
                f'USING {temp_table} s ON ({join_cond}) '
                f'WHEN MATCHED THEN UPDATE SET {update_set} '
                f'WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})'
            )

        # snowflake / ansi fallback
        return (
            f'MERGE INTO {target_table} AS t '
            f'USING {temp_table} AS s ON {join_cond} '
            f'WHEN MATCHED THEN UPDATE SET {update_set} '
            f'WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})'
        )

    # ── Upsert / Merge via temp table ─────────────────────────────────

    def _upsert(
        self,
        df,
        target_name: str,
        write_key: List[str],
        batchsize: int,
        spark,
    ) -> None:
        temp_table = f'_mkpipe_tmp_{target_name}'
        try:
            self._write_df(df, 'overwrite', temp_table, batchsize)
            sql = self._build_upsert_sql(
                temp_table,
                target_name,
                write_key,
                df.columns,
            )
            logger.debug({'upsert_sql': sql})
            self._execute_sql(sql, spark)
        finally:
            try:
                self._execute_sql(f'DROP TABLE IF EXISTS {temp_table}', spark)
            except Exception:
                logger.warning("Failed to drop temp table '%s'", temp_table)

    def _merge(
        self,
        df,
        target_name: str,
        write_key: List[str],
        batchsize: int,
        spark,
    ) -> None:
        temp_table = f'_mkpipe_tmp_{target_name}'
        non_key_cols = [c for c in df.columns if c not in write_key]
        try:
            self._write_df(df, 'overwrite', temp_table, batchsize)
            sql = self._build_merge_sql(
                temp_table,
                target_name,
                write_key,
                df.columns,
                non_key_cols,
            )
            logger.debug({'merge_sql': sql})
            self._execute_sql(sql, spark)
        finally:
            try:
                self._execute_sql(f'DROP TABLE IF EXISTS {temp_table}', spark)
            except Exception:
                logger.warning("Failed to drop temp table '%s'", temp_table)

    # ── Main load entry point ─────────────────────────────────────────

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        batchsize = table.batchsize
        df = data.df

        if df is None:
            logger.info(
                {
                    'table': target_name,
                    'status': 'skipped',
                    'reason': 'no data to load',
                }
            )
            return

        df = add_etl_columns(
            df, datetime.now(),
            dedup_columns=table.dedup_columns,
            ingested_at_column=self.ingested_at_column,
            ingestion_id_column=self.ingestion_id_column,
        )

        if table.write_partitions:
            df = df.coalesce(table.write_partitions)

        strategy = resolve_write_strategy(table, data)

        logger.info(
            {
                'table': target_name,
                'status': 'loading',
                'write_strategy': strategy.value,
            }
        )

        try:
            match strategy:
                case WriteStrategy.APPEND:
                    self._write_df(df, 'append', target_name, batchsize)
                case WriteStrategy.REPLACE:
                    self._write_df(df, 'overwrite', target_name, batchsize)
                case WriteStrategy.UPSERT:
                    if not table.write_key:
                        raise ConfigError(
                            f"write_strategy 'upsert' requires write_key for table '{target_name}'"
                        )
                    self._upsert(df, target_name, table.write_key, batchsize, spark)
                case WriteStrategy.MERGE:
                    if not table.write_key:
                        raise ConfigError(
                            f"write_strategy 'merge' requires write_key for table '{target_name}'"
                        )
                    self._merge(df, target_name, table.write_key, batchsize, spark)
                case _:
                    raise ConfigError(
                        f'JDBC loader does not support write_strategy: {strategy.value}'
                    )
        except (ConfigError, LoadError):
            raise
        except Exception as e:
            raise LoadError(f"Failed to write '{target_name}': {e}") from e

        logger.info(
            {
                'table': target_name,
                'status': 'loaded',
            }
        )
