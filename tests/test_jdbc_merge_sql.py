from __future__ import annotations

import pytest

from mkpipe.models import ConnectionConfig
from mkpipe.spark.jdbc_loader import JdbcLoader


def _make_loader(dialect: str = 'ansi') -> JdbcLoader:
    """Create a JdbcLoader with a fake connection for SQL generation tests."""
    conn = ConnectionConfig(variant='test', host='localhost', port=5432, database='testdb')
    loader = JdbcLoader(connection=conn)
    loader._dialect = dialect
    return loader


COLUMNS = ['id', 'name', 'email', 'etl_time', 'mkpipe_id']
WRITE_KEY = ['id']
UPDATE_COLS = ['name', 'email', 'etl_time', 'mkpipe_id']

MULTI_KEY = ['tenant_id', 'user_id']
MULTI_UPDATE = ['name', 'email', 'etl_time']


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
    def test_single_column_key_and_value(self):
        """Table with only a key column — update set should be empty string join."""
        loader = _make_loader('postgres')
        sql = loader._build_merge_sql('tmp', 'target', ['id'], ['id'], [])
        assert 'ON CONFLICT ("id") DO UPDATE SET ' in sql

    def test_all_columns_are_keys(self):
        """All columns are keys — no columns to update."""
        loader = _make_loader('sqlserver')
        cols = ['a', 'b']
        sql = loader._build_merge_sql('tmp', 'target', cols, cols, [])
        assert 'WHEN MATCHED THEN UPDATE SET ' in sql
