import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus
from typing import Dict, Optional
from zoneinfo import ZoneInfo

from .base import BaseExtractor
from ..exceptions import ConfigError
from ..models import ConnectionConfig, ExtractResult, TableConfig
from ..utils import get_logger

logger = get_logger(__name__)

# Column types that store true instants. Their watermarks must be stored and
# compared with an explicit UTC offset: a naive literal such as
# '2026-11-05 07:11:11.222000' is parsed by the source server in ITS session
# timezone (e.g. US/Eastern), silently shifting the incremental boundary by
# the UTC offset and skipping hours of rows.
_TZ_AWARE_DATA_TYPES = {'timestamp with time zone', 'timestamp_tz', 'timestamp_ltz'}

# Words that may appear inside iterate_column expressions but are never columns.
_SQL_RESERVED_WORDS = {
    'and',
    'as',
    'cast',
    'coalesce',
    'current_date',
    'current_timestamp',
    'date_trunc',
    'epoch',
    'extract',
    'from',
    'greatest',
    'least',
    'now',
    'or',
    'to_char',
    'where',
}

_IDENTIFIER_RE = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
# Matches a trailing explicit UTC offset ('+00', '-05:00', 'Z', ...).
_OFFSET_RE = re.compile(r'(?:Z|[+-]\d{2}(?::?\d{2})?)$')


def _session_timezone(spark) -> str:
    try:
        return spark.conf.get('spark.sql.session.timeZone') or 'UTC'
    except Exception:
        return 'UTC'


def _epoch_to_datetime(epoch_us: int) -> datetime:
    """Convert Spark epoch microseconds to a UTC datetime."""
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(epoch_us))


def _format_epoch_utc(epoch_us: int) -> str:
    """Render epoch microseconds as a UTC wall string with an explicit '+00'.

    The explicit '+00' suffix keeps the watermark immune to both the python
    process timezone (pyspark renders collected timestamps in the *local*
    zone, not the session zone) and the source server's session timezone
    when the value is embedded in WHERE clauses.
    """
    return _epoch_to_datetime(epoch_us).strftime('%Y-%m-%d %H:%M:%S.%f') + '+00'


def _format_epoch_wall(epoch_us: int, session_tz: str) -> str:
    """Render epoch microseconds as a naive wall string in session_tz.

    Naive source columns are interpreted by pgjdbc against the JVM timezone,
    which mkpipe pins to settings.timezone (== spark session timezone), so
    rendering the instant back in that zone recovers the original source
    wall clock regardless of the python process timezone.
    """
    try:
        zone = ZoneInfo(session_tz)
    except Exception:
        zone = timezone.utc
    return (
        _epoch_to_datetime(epoch_us)
        .astimezone(zone)
        .replace(tzinfo=None)
        .strftime('%Y-%m-%d %H:%M:%S.%f')
    )


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

    def _bounds_projection(self, df, *columns: str):
        """Project datetime bound columns to epoch microseconds (timezone-free).

        pyspark renders collected timestamps in the python process timezone
        (TimestampType.fromInternal uses local time), so naive datetimes are
        ambiguous. Converting inside Spark via unix_micros() keeps the value
        an unambiguous instant regardless of any timezone.
        """
        from pyspark.sql import functions as F

        return df.select(*[F.unix_micros(F.col(c)).alias(c) for c in columns])

    def _build_reader(
        self,
        spark,
        jdbc_url: str,
        dbtable: str,
        fetchsize: int = 0,
        partitions: int = 0,
        partition_column: str = None,
        lower_bound=None,
        upper_bound=None,
    ):
        reader = (
            spark.read.format('jdbc')
            .option('url', jdbc_url)
            .option('dbtable', dbtable)
            .option('driver', self.driver_jdbc)
        )
        if fetchsize:
            reader = reader.option('fetchsize', fetchsize)
        if partitions and partition_column:
            reader = (
                reader.option('numPartitions', partitions)
                .option('partitionColumn', partition_column)
                .option('lowerBound', lower_bound)
                .option('upperBound', upper_bound)
            )
        for k, v in self._jdbc_options().items():
            reader = reader.option(k, v)
        return reader.load()

    def _resolve_custom_query(
        self, table: TableConfig, root_dir: Optional[str] = None
    ) -> Optional[str]:
        if table.custom_query:
            return table.custom_query
        if table.custom_query_file:
            base = root_dir or os.getcwd()
            path = os.path.abspath(os.path.join(base, table.custom_query_file))
            with open(path, 'r') as f:
                return f.read()
        return None

    def _normalize_partitions_column(self, col: str) -> str:
        return col.split(' as ')[0].strip()

    def _unwrap_subquery(self, query: str) -> str:
        stripped = query.strip()
        m = re.match(r'^\((.*)\)\s*\w+\s*$', stripped, re.DOTALL)
        return m.group(1).strip() if m else stripped

    def _wrap_with_partition_alias(self, query: str, expr: str, alias: str) -> str:
        if re.search(rf'\bAS\s+{re.escape(alias)}\b', query, re.IGNORECASE):
            return query
        inner = self._unwrap_subquery(query)
        return f'(SELECT *, {expr} AS {alias} FROM ({inner}) _p) q'

    def _build_or_where(self, columns: list, condition_builder) -> str:
        if len(columns) == 1:
            return condition_builder(columns[0])
        parts = [f'({condition_builder(col)})' for col in columns]
        return ' OR '.join(parts)

    def _detect_iterate_columns_tz_aware(
        self, table: TableConfig, spark, jdbc_url: str
    ) -> Optional[bool]:
        """Resolve whether the iterate columns are timezone-aware.

        Returns True/False when the columns resolve against
        information_schema, and None when detection is not possible
        (unknown table, unresolvable expression columns, or a source
        without information_schema support).
        """
        candidates = {
            ident
            for expr in table.iterate_columns
            for ident in _IDENTIFIER_RE.findall(expr)
            if ident.lower() not in _SQL_RESERVED_WORDS
        }
        if not candidates:
            return None

        def _q(value: str) -> str:
            return "'" + value.replace("'", "''") + "'"

        name = table.name.strip()
        if '.' in name:
            schema_part, _, table_part = name.rpartition('.')
            schema_clause = f'table_schema = {_q(schema_part)} AND table_name = {_q(table_part)}'
        else:
            schema_clause = (
                f'table_schema = ANY (current_schemas(false)) AND table_name = {_q(name)}'
            )
        column_list = ', '.join(_q(c) for c in sorted(candidates))
        query = (
            f'(SELECT column_name, data_type FROM information_schema.columns '
            f'WHERE {schema_clause} AND column_name IN ({column_list})) q'
        )
        try:
            rows = self._build_reader(spark, jdbc_url, query).collect()
        except Exception as exc:
            logger.warning(
                {
                    'table': table.target_name,
                    'status': 'tz_detection_failed',
                    'error': str(exc),
                }
            )
            return None
        if not rows:
            return None
        return any(str(row[1]).lower() in _TZ_AWARE_DATA_TYPES for row in rows)

    def _extract_incremental(
        self, table: TableConfig, spark, last_point: Optional[str]
    ) -> ExtractResult:
        name = table.name
        iterate_column_type = table.iterate_column_type
        custom_query = self._resolve_custom_query(table)

        if not table.iterate_column:
            raise ConfigError(f"Table '{name}': incremental replication requires 'iterate_column'")

        columns = table.iterate_columns
        is_multi = table.is_multi_iterate_column

        if is_multi:
            iterate_col_normalized = f'GREATEST({", ".join(columns)})'
        else:
            iterate_col_normalized = self._normalize_partitions_column(columns[0])

        partitions_count = table.partitions_count
        partitions_column_raw = table.partitions_column or (
            columns[0] if not is_multi else iterate_col_normalized
        )
        partitions_column = self._normalize_partitions_column(partitions_column_raw)
        p_col_name = partitions_column_raw.split(' as ')[-1].strip()
        p_alias = p_col_name if ' as ' in partitions_column_raw.lower() else None
        fetchsize = table.fetchsize
        jdbc_url = self.build_jdbc_url()

        has_static_bounds = (
            table.filter_lower_bound is not None or table.filter_upper_bound is not None
        )

        # --- Step 1: Get iterate_column bounds (+ partition bounds in one query) ---
        if is_multi:
            min_exprs = ', '.join(f'min({c})' for c in columns)
            max_exprs = ', '.join(f'max({c})' for c in columns)
            min_select = f'LEAST({min_exprs}) AS min_val'
            max_select = f'GREATEST({max_exprs}) AS max_val'
        else:
            min_select = f'min({iterate_col_normalized}) AS min_val'
            max_select = f'max({iterate_col_normalized}) AS max_val'

        need_separate_p_bounds = partitions_count and partitions_column != iterate_col_normalized
        has_static_p_bounds = (
            table.partitions_lower_bound is not None and table.partitions_upper_bound is not None
        )

        if custom_query:
            bounds_cq = custom_query.replace('{query_filter}', ' WHERE 1=1 ')
            bounds_base_source = f'({self._unwrap_subquery(bounds_cq)}) _bounds_src'
        else:
            bounds_base_source = name

        if has_static_bounds:

            def _static_cond(col):
                parts = []
                if table.filter_lower_bound is not None:
                    if iterate_column_type == 'int':
                        parts.append(f'{col} >= {table.filter_lower_bound}')
                    else:
                        parts.append(f"{col} >= '{table.filter_lower_bound}'")
                if table.filter_upper_bound is not None:
                    if iterate_column_type == 'int':
                        parts.append(f'{col} < {table.filter_upper_bound}')
                    else:
                        parts.append(f"{col} < '{table.filter_upper_bound}'")
                return ' AND '.join(parts)

            where_clause = self._build_or_where(columns, _static_cond)
            bounds_query = (
                f'(SELECT {min_select}, {max_select} '
                f'FROM {bounds_base_source} WHERE {where_clause}) q'
            )
            write_mode = 'append'
        elif last_point:
            if iterate_column_type == 'int':
                lp_val = last_point
            else:
                lp_val = f"'{last_point}'"

            def _lp_cond(col):
                return f'{col} >= {lp_val}'

            where_clause = self._build_or_where(columns, _lp_cond)
            bounds_query = (
                f'(SELECT {min_select}, {max_select} '
                f'FROM {bounds_base_source} WHERE {where_clause}) q'
            )
            write_mode = 'append'
        else:
            where_clause = ''
            bounds_query = f'(SELECT {min_select}, {max_select} FROM {bounds_base_source}) q'
            write_mode = 'overwrite'

        df_bounds = self._build_reader(spark, jdbc_url, bounds_query)
        if iterate_column_type == 'datetime':
            df_bounds = self._bounds_projection(df_bounds, 'min_val', 'max_val')
        row = df_bounds.first()

        if not row or row[0] is None:
            if not last_point:
                logger.info({'table': table.target_name, 'status': 'empty_source_initial_load'})
                return self._extract_full(table, spark)
            logger.info({'table': table.target_name, 'status': 'no_new_data'})
            return ExtractResult(df=None, write_mode=write_mode)

        min_val, max_val = row[0], row[1]
        session_tz = _session_timezone(spark)

        if iterate_column_type == 'int':
            min_iterate = int(min_val)
            max_iterate = int(max_val)
        elif iterate_column_type == 'datetime':
            tz_mode = (table.iterate_column_tz or 'auto').lower()
            if tz_mode == 'aware':
                tz_aware = True
            elif tz_mode == 'naive':
                tz_aware = False
            else:
                tz_aware = self._detect_iterate_columns_tz_aware(table, spark, jdbc_url)
                if tz_aware is None:
                    tz_aware = False
                    logger.warning(
                        {
                            'table': table.target_name,
                            'status': 'tz_detection_unresolved',
                            'hint': "set iterate_column_tz: 'aware' or 'naive' explicitly",
                        }
                    )
            if tz_aware:
                min_iterate = _format_epoch_utc(min_val)
                max_iterate = _format_epoch_utc(max_val)
                if last_point and not _OFFSET_RE.search(last_point.strip()):
                    logger.warning(
                        {
                            'table': table.target_name,
                            'status': 'legacy_naive_last_point',
                            'last_point': last_point,
                            'hint': (
                                'timezone-aware iterate column but the stored '
                                'last_point has no UTC offset; it will be parsed '
                                'in the source server session timezone. Reset it '
                                "to '<max>+00' form to avoid skipped rows"
                            ),
                        }
                    )
            else:
                min_iterate = _format_epoch_wall(min_val, session_tz)
                max_iterate = _format_epoch_wall(max_val, session_tz)
        else:
            raise ConfigError(
                f"Table '{name}': unsupported iterate_column_type '{iterate_column_type}'. "
                f"Supported: 'int', 'datetime'"
            )

        # --- Step 2: Build filter clause using iterate_column ---
        if has_static_bounds or last_point:
            if iterate_column_type == 'int':

                def _range_cond(col):
                    return f'{col} >= {min_iterate} AND {col} <= {max_iterate}'
            else:

                def _range_cond(col):
                    return f"{col} >= '{min_iterate}' AND {col} <= '{max_iterate}'"

            range_expr = self._build_or_where(columns, _range_cond)
            filter_clause = f'WHERE {range_expr}'
        else:
            filter_clause = ''

        inject_alias = bool(p_alias and partitions_count)

        if custom_query:
            placeholder = f' {filter_clause} ' if filter_clause else ' WHERE 1=1 '
            updated_query = custom_query.replace('{query_filter}', placeholder)
            if inject_alias:
                updated_query = self._wrap_with_partition_alias(
                    updated_query, partitions_column, p_alias
                )
        elif inject_alias:
            updated_query = (
                f'(SELECT *, {partitions_column} AS {p_alias} FROM {name} {filter_clause}) q'
            )
        else:
            updated_query = f'(SELECT * FROM {name} {filter_clause}) q'

        # --- Step 3: Resolve partition bounds ---
        if table.partitions_column_type:
            p_col_type = table.partitions_column_type
        elif table.partitions_column:
            p_col_type = 'int'
        else:
            p_col_type = iterate_column_type

        if need_separate_p_bounds:
            if has_static_p_bounds:
                p_lower_raw = table.partitions_lower_bound
                p_upper_raw = table.partitions_upper_bound
            else:
                p_where = (
                    f' WHERE {where_clause}'
                    if where_clause and table.partitions_bounds_filtered
                    else ''
                )
                p_bounds_query = (
                    f'(SELECT min({partitions_column}) AS p_min, '
                    f'max({partitions_column}) AS p_max '
                    f'FROM {bounds_base_source}{p_where}) q'
                )
                df_p_bounds = self._build_reader(spark, jdbc_url, p_bounds_query)
                if p_col_type == 'datetime':
                    df_p_bounds = self._bounds_projection(df_p_bounds, 'p_min', 'p_max')
                p_row = df_p_bounds.first()
                p_lower_raw = p_row[0] if p_row and p_row[0] is not None else min_iterate
                p_upper_raw = p_row[1] if p_row and p_row[1] is not None else max_iterate

            if p_col_type == 'int':
                p_lower = int(float(str(p_lower_raw)))
                p_upper = int(float(str(p_upper_raw)))
            elif p_col_type == 'datetime':
                # Partition predicates never drop rows (the first/last Spark
                # JDBC partitions are unbounded), so only skew is at stake
                # when the timezone guess here is imperfect.
                p_is_aware = iterate_column_type == 'datetime' and tz_aware

                def _fmt_p(raw):
                    if isinstance(raw, str):
                        return raw
                    if p_is_aware:
                        return _format_epoch_utc(raw)
                    return _format_epoch_wall(raw, session_tz)

                p_lower = _fmt_p(p_lower_raw)
                p_upper = _fmt_p(p_upper_raw)
            else:
                p_lower = p_lower_raw
                p_upper = p_upper_raw
        else:
            p_lower = min_iterate
            p_upper = max_iterate

        df = self._build_reader(
            spark,
            jdbc_url,
            updated_query,
            fetchsize=fetchsize,
            partitions=partitions_count,
            partition_column=p_col_name,
            lower_bound=p_lower,
            upper_bound=p_upper,
        )

        if inject_alias:
            df = df.drop(p_alias)

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
        logger.info(
            {
                'table': table.target_name,
                'status': 'extracting',
                'replication_method': table.replication_method.value,
            }
        )

        if table.replication_method.value == 'incremental':
            result = self._extract_incremental(table, spark, last_point)
        else:
            result = self._extract_full(table, spark)

        logger.info(
            {
                'table': table.target_name,
                'status': 'extracted',
                'write_mode': result.write_mode,
            }
        )
        return result
