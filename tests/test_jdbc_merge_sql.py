from __future__ import annotations

import pytest

from mkpipe.exceptions import LoadError
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.spark.jdbc_loader import JdbcLoader


def _make_loader(dialect: str = 'ansi') -> JdbcLoader:
    """Create a JdbcLoader with a fake connection for SQL generation tests."""
    conn = ConnectionConfig(variant='test', host='localhost', port=5432, database='testdb')
    loader = JdbcLoader(connection=conn)
    loader._dialect = dialect
    return loader


COLUMNS = ['id', 'name', 'email', '_ingested_at', 'mkpipe_id']
WRITE_KEY = ['id']
UPDATE_COLS = ['name', 'email', '_ingested_at', 'mkpipe_id']

MULTI_KEY = ['tenant_id', 'user_id']
MULTI_UPDATE = ['name', 'email', '_ingested_at']


class TestBuildUpsertSql:
    def test_upsert_delegates_to_merge(self):
        loader = _make_loader('postgres')
        sql = loader._build_upsert_sql('tmp', 'target', WRITE_KEY, COLUMNS)
        assert 'ON CONFLICT' in sql
        assert 'EXCLUDED' in sql

    def test_upsert_non_key_cols_updated(self):
        loader = _make_loader('postgres')
        sql = loader._build_upsert_sql('tmp', 'target', WRITE_KEY, COLUMNS)
        for col in UPDATE_COLS:
            assert f'"{col}" = EXCLUDED."{col}"' in sql
        # Key column should NOT be in the update set
        assert '"id" = EXCLUDED."id"' not in sql


class TestPostgresDialect:
    def test_on_conflict_single_key(self):
        loader = _make_loader('postgres')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert sql.startswith('INSERT INTO target')
        assert 'ON CONFLICT ("id")' in sql
        assert 'DO UPDATE SET' in sql
        assert 'EXCLUDED' in sql

    def test_on_conflict_multi_key(self):
        loader = _make_loader('postgres')
        sql = loader._build_merge_sql('tmp', 'target', MULTI_KEY, COLUMNS, MULTI_UPDATE)
        assert 'ON CONFLICT ("tenant_id", "user_id")' in sql

    def test_select_from_temp(self):
        loader = _make_loader('postgres')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert 'SELECT' in sql
        assert 'FROM tmp' in sql


class TestRedshiftDialect:
    def test_same_as_postgres(self):
        loader = _make_loader('redshift')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert 'ON CONFLICT' in sql
        assert 'EXCLUDED' in sql


class TestTimescaledbDialect:
    def test_same_as_postgres(self):
        loader = _make_loader('timescaledb')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert 'ON CONFLICT' in sql


class TestSqliteDialect:
    def test_same_as_postgres(self):
        loader = _make_loader('sqlite')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert 'ON CONFLICT' in sql


class TestMysqlDialect:
    def test_on_duplicate_key(self):
        loader = _make_loader('mysql')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert 'ON DUPLICATE KEY UPDATE' in sql
        assert 'VALUES(' in sql.replace(' ', '')

    def test_backtick_quoting(self):
        loader = _make_loader('mysql')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert '`name`' in sql
        assert '`email`' in sql


class TestMariadbDialect:
    def test_on_duplicate_key(self):
        loader = _make_loader('mariadb')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert 'ON DUPLICATE KEY UPDATE' in sql


class TestSqlserverDialect:
    def test_merge_syntax(self):
        loader = _make_loader('sqlserver')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert sql.startswith('MERGE target AS t')
        assert 'USING tmp AS s' in sql
        assert 'WHEN MATCHED THEN UPDATE SET' in sql
        assert 'WHEN NOT MATCHED THEN INSERT' in sql
        assert sql.endswith(';')

    def test_join_condition(self):
        loader = _make_loader('sqlserver')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert 't."id" = s."id"' in sql

    def test_multi_key_join(self):
        loader = _make_loader('sqlserver')
        sql = loader._build_merge_sql('tmp', 'target', MULTI_KEY, COLUMNS, MULTI_UPDATE)
        assert 't."tenant_id" = s."tenant_id"' in sql
        assert 't."user_id" = s."user_id"' in sql


class TestOracleDialect:
    def test_merge_into_syntax(self):
        loader = _make_loader('oracledb')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert sql.startswith('MERGE INTO target t')
        assert 'USING tmp s ON' in sql
        assert 'WHEN MATCHED THEN UPDATE SET' in sql
        assert 'WHEN NOT MATCHED THEN INSERT' in sql

    def test_join_in_parens(self):
        loader = _make_loader('oracledb')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert 'ON (t."id" = s."id")' in sql


class TestAnsiDialect:
    def test_merge_into_as(self):
        loader = _make_loader('ansi')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert 'MERGE INTO target AS t' in sql
        assert 'USING tmp AS s' in sql

    def test_snowflake_uses_ansi(self):
        loader = _make_loader('snowflake')
        sql = loader._build_merge_sql('tmp', 'target', WRITE_KEY, COLUMNS, UPDATE_COLS)
        assert 'MERGE INTO target AS t' in sql


class TestEdgeCases:
    @pytest.mark.parametrize(
        'dialect', ['postgres', 'postgresql', 'redshift', 'timescaledb', 'sqlite']
    )
    def test_key_only_conflict_does_nothing(self, dialect):
        loader = _make_loader(dialect)
        sql = loader._build_upsert_sql('tmp', 'target', ['id'], ['id'])
        assert sql.endswith('ON CONFLICT ("id") DO NOTHING')

    @pytest.mark.parametrize('dialect', ['sqlserver', 'oracle', 'oracledb', 'ansi', 'snowflake'])
    def test_key_only_merge_inserts_unmatched_rows(self, dialect):
        loader = _make_loader(dialect)
        cols = ['a', 'b']
        sql = loader._build_merge_sql('tmp', 'target', cols, cols, [])
        assert 'WHEN MATCHED' not in sql
        assert 'WHEN NOT MATCHED THEN INSERT' in sql

    @pytest.mark.parametrize('dialect', ['mysql', 'mariadb'])
    def test_key_only_duplicate_preserves_matched_row(self, dialect):
        loader = _make_loader(dialect)
        sql = loader._build_upsert_sql('tmp', 'target', ['id'], ['id'])
        assert sql.endswith('ON DUPLICATE KEY UPDATE `id` = target.`id`')


class _FakeDataFrame:
    def __init__(self, columns):
        self.columns = columns
        self.selected = None

    def select(self, *columns):
        self.selected = list(columns)
        return self


class TestIgnoreExtraSourceColumns:
    def test_direct_loader_does_not_reuse_previous_table_override(self, monkeypatch):
        loader = _make_loader('postgres')
        monkeypatch.setattr('mkpipe.spark.jdbc_loader.add_etl_columns', lambda df, *a, **kw: df)
        monkeypatch.setattr(loader, '_table_exists', lambda *a: True)
        monkeypatch.setattr(loader, '_target_columns', lambda *a: ['id'])
        monkeypatch.setattr(loader, '_write_df', lambda *a: None)

        first = _FakeDataFrame(['id', 'extra'])
        second = _FakeDataFrame(['id', 'extra'])
        table = TableConfig(name='src', target_name='dst')
        loader.load(
            table.model_copy(update={'ignore_extra_source_columns': True}),
            ExtractResult(df=first, write_mode='append'),
            None,
        )
        loader.load(table, ExtractResult(df=second, write_mode='append'), None)
        assert first.selected == ['id']
        assert second.selected is None

    def test_target_schema_query_is_metadata_only(self):
        loader = _make_loader('postgres')

        class Reader:
            def __init__(self):
                self.options = {}

            def format(self, value):
                assert value == 'jdbc'
                return self

            def option(self, key, value):
                self.options[key] = value
                return self

            def load(self):
                return type('Frame', (), {'columns': ['id', 'name']})()

        class Spark:
            read = Reader()

        assert loader._target_columns('stg_users', Spark()) == ['id', 'name']
        assert 'WHERE 1 = 0' in Spark.read.options['dbtable']

    def test_selects_only_target_columns_and_logs_extras(self, caplog):
        loader = _make_loader('postgres')
        df = _FakeDataFrame(['id', 'name', 'new_source_col'])

        result = loader._align_to_target_columns(
            df,
            target_name='stg_users',
            target_columns=['id', 'name', '_ingested_at'],
        )

        assert result is df
        assert df.selected == ['id', 'name']
        assert 'new_source_col' in caplog.text
        assert 'WARNING' in caplog.text

    def test_missing_write_key_in_target_raises(self):
        loader = _make_loader('postgres')
        df = _FakeDataFrame(['id', 'name'])

        with pytest.raises(LoadError, match='write_key.*missing from target'):
            loader._align_to_target_columns(
                df,
                target_name='stg_users',
                target_columns=['name'],
                required_columns=['id'],
            )

    def test_no_shared_columns_raises(self):
        loader = _make_loader('postgres')
        df = _FakeDataFrame(['new_source_col'])

        with pytest.raises(LoadError, match='no columns in common'):
            loader._align_to_target_columns(
                df,
                target_name='stg_users',
                target_columns=['id', 'name'],
            )
