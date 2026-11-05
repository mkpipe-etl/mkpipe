from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pytest

from mkpipe.models import ConnectionConfig, TableConfig
from mkpipe.spark.jdbc_extractor import (
    JdbcExtractor,
    _format_epoch_utc,
    _format_epoch_wall,
)

_EPOCH = datetime(1970, 1, 1)


def _epoch_us(dt: datetime) -> int:
    delta = dt - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


class FakeDataFrame:
    def __init__(self, row: Optional[List[Any]] = None):
        self._row = row
        self.dropped: List[str] = []

    def first(self):
        return self._row

    def collect(self) -> List[Any]:
        return [self._row] if self._row is not None else []

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

    def _bounds_projection(self, df, *columns):
        # Unit tests feed epoch-microsecond rows directly; the real
        # unix_micros projection needs a live Spark session and is covered
        # by the docker integration test.
        return df

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


def _bounds_row() -> List[int]:
    return [_epoch_us(datetime(2026, 7, 28, 1, 38, 6)), _epoch_us(datetime(2026, 8, 5, 12, 0, 0))]


def _naive_tz_row() -> List[str]:
    return ['cdate', 'timestamp without time zone']


def _aware_tz_row() -> List[str]:
    return ['cdate', 'timestamp with time zone']


def _table(**overrides: Any) -> TableConfig:
    defaults: Dict[str, Any] = {
        'name': 'apld_bill_rt_tax',
        'target_name': 'raw_apld_bill_rt_tax',
        'replication_method': 'incremental',
        'iterate_column': 'greatest(cdate,udate)',
        'iterate_column_type': 'datetime',
        'partitions_column': 'acct_bill_id',
        'partitions_count': 4,
    }
    defaults.update(overrides)
    return TableConfig(**defaults)


def test_bounds_query_excludes_partition_aggregates(connection):
    extractor = RecordingExtractor(
        connection, [_bounds_row(), _naive_tz_row(), [1, 900_000_000], None]
    )

    extractor.extract(_table(), spark=None, last_point='2026-07-28 01:38:06.816212')

    bounds_query = extractor.queries[0]
    assert 'min(greatest(cdate,udate))' in bounds_query
    assert 'max(greatest(cdate,udate))' in bounds_query
    assert 'acct_bill_id' not in bounds_query


def test_partition_bounds_query_is_unfiltered_by_default(connection):
    extractor = RecordingExtractor(
        connection, [_bounds_row(), _naive_tz_row(), [1, 900_000_000], None]
    )

    extractor.extract(_table(), spark=None, last_point='2026-07-28 01:38:06.816212')

    p_query = extractor.queries[2]
    assert 'min(acct_bill_id)' in p_query
    assert 'max(acct_bill_id)' in p_query
    assert 'WHERE' not in p_query
    assert extractor.calls[-1]['lower_bound'] == 1
    assert extractor.calls[-1]['upper_bound'] == 900_000_000


def test_partition_bounds_filtered_applies_where(connection):
    extractor = RecordingExtractor(connection, [_bounds_row(), _naive_tz_row(), [10, 20], None])

    extractor.extract(
        _table(partitions_bounds_filtered=True),
        spark=None,
        last_point='2026-07-28 01:38:06.816212',
    )

    p_query = extractor.queries[2]
    assert "WHERE greatest(cdate,udate) >= '2026-07-28 01:38:06.816212'" in p_query


def test_static_partition_bounds_skip_the_query(connection):
    extractor = RecordingExtractor(
        connection,
        [_bounds_row(), _naive_tz_row(), None],
    )

    extractor.extract(
        _table(
            partitions_lower_bound=800_000_000,
            partitions_upper_bound=900_000_000,
        ),
        spark=None,
        last_point='2026-07-28 01:38:06.816212',
    )

    assert len(extractor.queries) == 3
    assert extractor.calls[-1]['lower_bound'] == 800_000_000
    assert extractor.calls[-1]['upper_bound'] == 900_000_000


def test_alias_partition_column_injects_and_drops_column(connection):
    extractor = RecordingExtractor(connection, [_bounds_row(), _naive_tz_row(), None])

    result = extractor.extract(
        _table(
            partitions_column='greatest(cdate,udate) as _part_ts',
            partitions_column_type='datetime',
        ),
        spark=None,
        last_point='2026-07-28 01:38:06.816212',
    )

    assert len(extractor.queries) == 3
    data_query = extractor.queries[2]
    assert 'greatest(cdate,udate) AS _part_ts' in data_query
    assert extractor.calls[-1]['partition_column'] == '_part_ts'
    assert extractor.calls[-1]['lower_bound'] == '2026-07-28 01:38:06.000000'
    assert extractor.calls[-1]['upper_bound'] == '2026-08-05 12:00:00.000000'
    assert result.df.dropped == ['_part_ts']


def test_custom_query_is_wrapped_with_partition_alias(connection):
    extractor = RecordingExtractor(connection, [_bounds_row(), _naive_tz_row(), None])

    result = extractor.extract(
        _table(
            partitions_column='greatest(cdate,udate) as partts',
            partitions_column_type='datetime',
            custom_query=(
                '(\n    SELECT *\n    FROM (\n        SELECT * FROM ctx_aggr_rcrds\n'
                "        WHERE period_retention_date >= date_trunc('month', current_date)\n"
                '    ) t\n    {query_filter}\n) q'
            ),
        ),
        spark=None,
        last_point='2026-07-28 01:38:06.816212',
    )

    data_query = extractor.queries[2]
    assert data_query.startswith('(SELECT *, greatest(cdate,udate) AS partts FROM (')
    assert data_query.endswith(') _p) q')
    assert "WHERE period_retention_date >= date_trunc('month', current_date)" in data_query
    assert extractor.calls[-1]['partition_column'] == 'partts'
    assert result.df.dropped == ['partts']


def test_custom_query_already_exposing_alias_is_not_rewrapped(connection):
    extractor = RecordingExtractor(connection, [_bounds_row(), _naive_tz_row(), None])

    result = extractor.extract(
        _table(
            partitions_column='greatest(cdate,udate) as partts',
            partitions_column_type='datetime',
            custom_query=(
                '(SELECT *, greatest(cdate,udate) AS partts FROM apld_bill_rt_tax {query_filter}) q'
            ),
        ),
        spark=None,
        last_point='2026-07-28 01:38:06.816212',
    )

    data_query = extractor.queries[2]
    assert data_query.count('AS partts') == 1
    assert '_p) q' not in data_query
    assert result.df.dropped == ['partts']


def test_format_epoch_helpers_are_timezone_exact():
    us = _epoch_us(datetime(2026, 11, 5, 7, 11, 11, 222000))
    assert _format_epoch_utc(us) == '2026-11-05 07:11:11.222000+00'
    # EST (UTC-5): 07:11 UTC == 02:11 America/Toronto wall
    assert _format_epoch_wall(us, 'America/Toronto') == '2026-11-05 02:11:11.222000'
    # EDT (UTC-4): 07:11 UTC == 03:11 America/Toronto wall
    us_summer = _epoch_us(datetime(2026, 7, 5, 7, 11, 11, 222000))
    assert _format_epoch_wall(us_summer, 'America/Toronto') == '2026-07-05 03:11:11.222000'
    # Unknown zone falls back to UTC rather than crashing
    assert _format_epoch_wall(us, 'Not/AZone') == '2026-11-05 07:11:11.222000'


def test_tz_aware_columns_use_utc_offset_watermark(connection):
    extractor = RecordingExtractor(connection, [_bounds_row(), _aware_tz_row(), [1, 100], None])

    result = extractor.extract(_table(), spark=None, last_point='2026-07-28 01:38:06.816212+00')

    assert result.last_point_value == '2026-08-05 12:00:00.000000+00'
    bounds_query = extractor.queries[0]
    assert "greatest(cdate,udate) >= '2026-07-28 01:38:06.816212+00'" in bounds_query
    data_query = extractor.queries[-1]
    assert "greatest(cdate,udate) >= '2026-07-28 01:38:06.000000+00'" in data_query
    assert "greatest(cdate,udate) <= '2026-08-05 12:00:00.000000+00'" in data_query


def test_legacy_naive_last_point_embedded_in_bounds_query(connection):
    extractor = RecordingExtractor(connection, [_bounds_row(), _aware_tz_row(), [1, 100], None])

    result = extractor.extract(_table(), spark=None, last_point='2026-07-28 01:38:06.816212')

    # Legacy naive last_point is embedded as-is in the bounds query (parsed
    # server-side, hence the warning); the data query still re-ranges from
    # the matched rows' own min/max with explicit offsets.
    bounds_query = extractor.queries[0]
    assert "greatest(cdate,udate) >= '2026-07-28 01:38:06.816212'" in bounds_query
    data_query = extractor.queries[-1]
    assert "greatest(cdate,udate) >= '2026-07-28 01:38:06.000000+00'" in data_query
    assert result.last_point_value == '2026-08-05 12:00:00.000000+00'


def test_iterate_column_tz_override_skips_detection(connection):
    extractor = RecordingExtractor(connection, [_bounds_row(), None])

    result = extractor.extract(
        _table(partitions_count=0, iterate_column_tz='aware'),
        spark=None,
        last_point=None,
    )

    assert len(extractor.queries) == 2
    assert result.last_point_value == '2026-08-05 12:00:00.000000+00'

    extractor = RecordingExtractor(connection, [_bounds_row(), None])

    result = extractor.extract(
        _table(partitions_count=0, iterate_column_tz='naive'),
        spark=None,
        last_point=None,
    )

    assert len(extractor.queries) == 2
    assert result.last_point_value == '2026-08-05 12:00:00.000000'


def test_detection_unresolved_falls_back_to_naive(connection):
    extractor = RecordingExtractor(connection, [_bounds_row(), None, None])

    result = extractor.extract(
        _table(partitions_count=0), spark=None, last_point='2026-07-28 01:38:06.816212'
    )

    assert result.last_point_value == '2026-08-05 12:00:00.000000'


def test_detection_query_targets_information_schema(connection):
    extractor = RecordingExtractor(connection, [_bounds_row(), _aware_tz_row(), [1, 100], None])

    extractor.extract(_table(), spark=None, last_point='2026-07-28 01:38:06.816212+00')

    detection_query = extractor.queries[1]
    assert 'information_schema.columns' in detection_query
    assert 'current_schemas(false)' in detection_query
    assert "IN ('cdate', 'udate')" in detection_query


def test_int_iterate_column_skips_tz_detection(connection):
    extractor = RecordingExtractor(connection, [[5, 900], None])

    extractor.extract(
        _table(iterate_column='acct_bill_id', iterate_column_type='int'),
        spark=None,
        last_point='100',
    )

    assert len(extractor.queries) == 2
    assert 'information_schema' not in extractor.queries[0]


def test_legacy_naive_last_point_on_aware_column_warns(connection, caplog):
    extractor = RecordingExtractor(connection, [_bounds_row(), _aware_tz_row(), [1, 100], None])

    with caplog.at_level('WARNING', logger='mkpipe.spark.jdbc_extractor'):
        extractor.extract(_table(), spark=None, last_point='2026-07-28 01:38:06.816212')

    assert 'legacy_naive_last_point' in caplog.text
