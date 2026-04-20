from __future__ import annotations

import pytest

from mkpipe.models import ExtractResult, TableConfig, WriteStrategy
from mkpipe.strategy import resolve_write_strategy


def _make_table(**kwargs) -> TableConfig:
    defaults = {'name': 'test_table', 'target_name': 'test_target'}
    defaults.update(kwargs)
    return TableConfig(**defaults)


def _make_result(write_mode: str = 'overwrite') -> ExtractResult:
    return ExtractResult(df=None, write_mode=write_mode)


# --- resolve_write_strategy tests ---


class TestResolveWriteStrategy:
    def test_explicit_append(self):
        table = _make_table(write_strategy='append')
        result = _make_result('overwrite')
        assert resolve_write_strategy(table, result) == WriteStrategy.APPEND

    def test_explicit_replace(self):
        table = _make_table(write_strategy='replace')
        result = _make_result('append')
        assert resolve_write_strategy(table, result) == WriteStrategy.REPLACE

    def test_explicit_upsert(self):
        table = _make_table(write_strategy='upsert', write_key=['id'])
        result = _make_result('append')
        assert resolve_write_strategy(table, result) == WriteStrategy.UPSERT

    def test_explicit_merge(self):
        table = _make_table(write_strategy='merge', write_key=['id'])
        result = _make_result('append')
        assert resolve_write_strategy(table, result) == WriteStrategy.MERGE

    def test_infer_replace_from_overwrite(self):
        table = _make_table()
        result = _make_result('overwrite')
        assert resolve_write_strategy(table, result) == WriteStrategy.REPLACE

    def test_infer_append_from_append(self):
        table = _make_table()
        result = _make_result('append')
        assert resolve_write_strategy(table, result) == WriteStrategy.APPEND

    def test_explicit_overrides_write_mode(self):
        table = _make_table(write_strategy='append')
        result = _make_result('overwrite')
        # explicit write_strategy should win over write_mode
        assert resolve_write_strategy(table, result) == WriteStrategy.APPEND


# --- TableConfig validation tests ---


class TestTableConfigValidation:
    def test_upsert_without_write_key_raises(self):
        with pytest.raises(ValueError, match='requires write_key'):
            _make_table(write_strategy='upsert')

    def test_merge_without_write_key_raises(self):
        with pytest.raises(ValueError, match='requires write_key'):
            _make_table(write_strategy='merge')

    def test_upsert_with_write_key_ok(self):
        table = _make_table(write_strategy='upsert', write_key=['mkpipe_id'])
        assert table.write_strategy == WriteStrategy.UPSERT
        assert table.write_key == ['mkpipe_id']

    def test_merge_with_write_key_ok(self):
        table = _make_table(write_strategy='merge', write_key=['id'])
        assert table.write_strategy == WriteStrategy.MERGE

    def test_append_without_write_key_ok(self):
        table = _make_table(write_strategy='append')
        assert table.write_strategy == WriteStrategy.APPEND
        assert table.write_key is None

    def test_replace_without_write_key_ok(self):
        table = _make_table(write_strategy='replace')
        assert table.write_strategy == WriteStrategy.REPLACE

    def test_no_write_strategy_backward_compat(self):
        table = _make_table()
        assert table.write_strategy is None
        assert table.write_key is None

    def test_write_strategy_none_with_dedup_columns(self):
        table = _make_table(dedup_columns=['col_a', 'col_b'])
        assert table.write_strategy is None
        assert table.dedup_columns == ['col_a', 'col_b']


# --- WriteStrategy enum tests ---


class TestWriteStrategyEnum:
    def test_values(self):
        assert WriteStrategy.APPEND.value == 'append'
        assert WriteStrategy.REPLACE.value == 'replace'
        assert WriteStrategy.UPSERT.value == 'upsert'
        assert WriteStrategy.MERGE.value == 'merge'

    def test_from_string(self):
        assert WriteStrategy('append') == WriteStrategy.APPEND
        assert WriteStrategy('upsert') == WriteStrategy.UPSERT

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            WriteStrategy('invalid')
