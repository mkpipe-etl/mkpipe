from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pytest

from mkpipe.models import ConnectionConfig, TableConfig
from mkpipe.spark.jdbc_extractor import JdbcExtractor


class FakeDataFrame:
    def __init__(self, row: Optional[List[Any]] = None):
        self._row = row
        self.dropped: List[str] = []

    def first(self):
        return self._row

    def drop(self, column: str) -> 'FakeDataFrame':
        self.dropped.append(column)
        return self


class RecordingExtractor(JdbcExtractor):
    driver_name = 'postgresql'
    driver_jdbc = 'org.postgresql.Driver'

    def __init__(self, connection: ConnectionConfig, rows: List[Optional[List[Any]]]):
        super().__init__(connection)
        self._rows = list(rows)
        self.calls: List[Dict[str, Any]] = []
        self.frames: List[FakeDataFrame] = []

    def _build_reader(self, spark, jdbc_url, dbtable, **kwargs):
        self.calls.append({'dbtable': dbtable, **kwargs})
        row = self._rows.pop(0) if self._rows else None
        frame = FakeDataFrame(row)
        self.frames.append(frame)
        return frame

    @property
    def queries(self) -> List[str]:
        return [c['dbtable'] for c in self.calls]


@pytest.fixture
def connection() -> ConnectionConfig:
    return ConnectionConfig(
        variant='postgresql',
        host='localhost',
        port=5432,
        database='db',
        user='u',
        password='p',
    )


def _bounds_row() -> List[Any]:
    return [datetime(2026, 7, 28, 1, 38, 6), datetime(2026, 8, 5, 12, 0, 0)]


def test_bounds_query_excludes_partition_aggregates(connection):
    table = TableConfig(
        name='apld_bill_rt_tax',
        target_name='raw_apld_bill_rt_tax',
        replication_method='incremental',
        iterate_column='greatest(cdate,udate)',
        iterate_column_type='datetime',
        partitions_column='acct_bill_id',
        partitions_count=4,
    )
    extractor = RecordingExtractor(connection, [_bounds_row(), [1, 900_000_000], None])

    extractor.extract(table, spark=None, last_point='2026-07-28 01:38:06.816212')

    bounds_query = extractor.queries[0]
    assert 'min(greatest(cdate,udate))' in bounds_query
    assert 'max(greatest(cdate,udate))' in bounds_query
    assert 'acct_bill_id' not in bounds_query


def test_partition_bounds_query_is_unfiltered_by_default(connection):
    table = TableConfig(
        name='apld_bill_rt_tax',
        target_name='raw_apld_bill_rt_tax',
        replication_method='incremental',
        iterate_column='greatest(cdate,udate)',
        iterate_column_type='datetime',
        partitions_column='acct_bill_id',
        partitions_count=4,
    )
    extractor = RecordingExtractor(connection, [_bounds_row(), [1, 900_000_000], None])

    extractor.extract(table, spark=None, last_point='2026-07-28 01:38:06.816212')

    p_query = extractor.queries[1]
    assert 'min(acct_bill_id)' in p_query
    assert 'max(acct_bill_id)' in p_query
    assert 'WHERE' not in p_query
    assert extractor.calls[-1]['lower_bound'] == 1
    assert extractor.calls[-1]['upper_bound'] == 900_000_000


def test_partition_bounds_filtered_applies_where(connection):
    table = TableConfig(
        name='apld_bill_rt_tax',
        target_name='raw_apld_bill_rt_tax',
        replication_method='incremental',
        iterate_column='greatest(cdate,udate)',
        iterate_column_type='datetime',
        partitions_column='acct_bill_id',
        partitions_count=4,
        partitions_bounds_filtered=True,
    )
    extractor = RecordingExtractor(connection, [_bounds_row(), [10, 20], None])

    extractor.extract(table, spark=None, last_point='2026-07-28 01:38:06.816212')

    p_query = extractor.queries[1]
    assert "WHERE greatest(cdate,udate) >= '2026-07-28 01:38:06.816212'" in p_query


def test_static_partition_bounds_skip_the_query(connection):
    table = TableConfig(
        name='apld_bill_rt_tax',
        target_name='raw_apld_bill_rt_tax',
        replication_method='incremental',
        iterate_column='greatest(cdate,udate)',
        iterate_column_type='datetime',
        partitions_column='acct_bill_id',
        partitions_count=4,
        partitions_lower_bound=800_000_000,
        partitions_upper_bound=900_000_000,
    )
    extractor = RecordingExtractor(connection, [_bounds_row(), None])

    extractor.extract(table, spark=None, last_point='2026-07-28 01:38:06.816212')

    assert len(extractor.queries) == 2
    assert extractor.calls[-1]['lower_bound'] == 800_000_000
    assert extractor.calls[-1]['upper_bound'] == 900_000_000


def test_alias_partition_column_injects_and_drops_column(connection):
    table = TableConfig(
        name='apld_bill_rt_tax',
        target_name='raw_apld_bill_rt_tax',
        replication_method='incremental',
        iterate_column='greatest(cdate,udate)',
        iterate_column_type='datetime',
        partitions_column='greatest(cdate,udate) as _part_ts',
        partitions_column_type='datetime',
        partitions_count=4,
    )
    extractor = RecordingExtractor(connection, [_bounds_row(), None])

    result = extractor.extract(table, spark=None, last_point='2026-07-28 01:38:06.816212')

    assert len(extractor.queries) == 2
    data_query = extractor.queries[1]
    assert 'greatest(cdate,udate) AS _part_ts' in data_query
    assert extractor.calls[-1]['partition_column'] == '_part_ts'
    assert extractor.calls[-1]['lower_bound'] == '2026-07-28 01:38:06.000000'
    assert extractor.calls[-1]['upper_bound'] == '2026-08-05 12:00:00.000000'
    assert result.df.dropped == ['_part_ts']


def test_custom_query_is_wrapped_with_partition_alias(connection):
    table = TableConfig(
        name='ctx_aggr_rcrds',
        target_name='raw_ctx_aggr_rcrds',
        replication_method='incremental',
        iterate_column='greatest(cdate,udate)',
        iterate_column_type='datetime',
        partitions_column='greatest(cdate,udate) as partts',
        partitions_column_type='datetime',
        partitions_count=4,
        custom_query=(
            '(\n    SELECT *\n    FROM (\n        SELECT * FROM ctx_aggr_rcrds\n'
            "        WHERE period_retention_date >= date_trunc('month', current_date)\n"
            '    ) t\n    {query_filter}\n) q'
        ),
    )
    extractor = RecordingExtractor(connection, [_bounds_row(), None])

    result = extractor.extract(table, spark=None, last_point='2026-07-28 01:38:06.816212')

    data_query = extractor.queries[1]
    assert data_query.startswith('(SELECT *, greatest(cdate,udate) AS partts FROM (')
    assert data_query.endswith(') _p) q')
    assert "WHERE period_retention_date >= date_trunc('month', current_date)" in data_query
    assert extractor.calls[-1]['partition_column'] == 'partts'
    assert result.df.dropped == ['partts']


def test_custom_query_already_exposing_alias_is_not_rewrapped(connection):
    table = TableConfig(
        name='apld_bill_rt_tax',
        target_name='raw_apld_bill_rt_tax',
        replication_method='incremental',
        iterate_column='greatest(cdate,udate)',
        iterate_column_type='datetime',
        partitions_column='greatest(cdate,udate) as partts',
        partitions_column_type='datetime',
        partitions_count=4,
        custom_query=(
            '(SELECT *, greatest(cdate,udate) AS partts '
            'FROM apld_bill_rt_tax {query_filter}) q'
        ),
    )
    extractor = RecordingExtractor(connection, [_bounds_row(), None])

    result = extractor.extract(table, spark=None, last_point='2026-07-28 01:38:06.816212')

    data_query = extractor.queries[1]
    assert data_query.count('AS partts') == 1
    assert '_p) q' not in data_query
    assert result.df.dropped == ['partts']
