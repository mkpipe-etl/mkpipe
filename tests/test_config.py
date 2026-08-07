from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from mkpipe.config import load_config
from mkpipe.exceptions import ConfigError
from mkpipe.models import BackendConfig, SparkConfig


@pytest.fixture
def write_config(tmp_path: Path):
    def _write(env_data: dict, *, environment: str = 'prod') -> Path:
        path = tmp_path / 'mkpipe_project.yaml'
        path.write_text(
            yaml.safe_dump({'default_environment': environment, environment: env_data})
        )
        return path

    return _write


def test_settings_if_exists_is_read_from_yaml(write_config):
    """Regression: settings.if_exists used to be dropped by load_config."""
    path = write_config({'settings': {'if_exists': 'append'}})

    cfg = load_config(path)

    assert cfg.settings.if_exists == 'append'


def test_settings_defaults_when_block_missing(write_config):
    path = write_config({})

    cfg = load_config(path)

    assert cfg.settings.timezone == 'UTC'
    assert cfg.settings.log_dir is None
    assert cfg.settings.ingested_at_column == '_ingested_at'
    assert cfg.settings.ingestion_id_column == 'mkpipe_id'
    assert cfg.settings.column_name_case == 'as_is'
    assert cfg.settings.if_exists == 'replace'
    assert cfg.settings.backend.variant == 'sqlite'
    assert cfg.settings.spark.master is None


def test_settings_partial_override_keeps_other_defaults(write_config):
    path = write_config({'settings': {'timezone': 'Europe/Istanbul'}})

    cfg = load_config(path)

    assert cfg.settings.timezone == 'Europe/Istanbul'
    assert cfg.settings.if_exists == 'replace'
    assert cfg.settings.ingestion_id_column == 'mkpipe_id'


def test_settings_null_block_falls_back_to_defaults(write_config):
    path = write_config({'settings': None})

    cfg = load_config(path)

    assert cfg.settings.timezone == 'UTC'


def test_nested_backend_and_spark_are_coerced_to_models(write_config):
    path = write_config(
        {
            'settings': {
                'backend': {'variant': 'postgres', 'host': 'localhost', 'port': 5432},
                'spark': {'master': 'local[4]', 'extra_config': {'spark.foo': 'bar'}},
            }
        }
    )

    cfg = load_config(path)

    assert isinstance(cfg.settings.backend, BackendConfig)
    assert cfg.settings.backend.variant == 'postgres'
    assert cfg.settings.backend.port == 5432
    assert isinstance(cfg.settings.spark, SparkConfig)
    assert cfg.settings.spark.master == 'local[4]'
    assert cfg.settings.spark.extra_config == {'spark.foo': 'bar'}


def test_unknown_settings_key_is_ignored(write_config):
    path = write_config({'settings': {'if_exists': 'append', 'not_a_real_setting': 1}})

    cfg = load_config(path)

    assert cfg.settings.if_exists == 'append'
    assert not hasattr(cfg.settings, 'not_a_real_setting')


def test_pipelines_and_tables_are_loaded(write_config):
    path = write_config(
        {
            'settings': {'if_exists': 'append'},
            'connections': {
                'src': {'variant': 'postgresql', 'host': 'h'},
                'dst': {'variant': 'iceberg'},
            },
            'pipelines': [
                {
                    'name': 'p1',
                    'source': 'src',
                    'destination': 'dst',
                    'pass_on_error': True,
                    'tables': [{'name': 'public.users', 'target_name': 'stg_users'}],
                }
            ],
        }
    )

    cfg = load_config(path)

    assert cfg.settings.if_exists == 'append'
    assert cfg.connections['src'].variant == 'postgresql'
    assert cfg.pipelines[0].pass_on_error is True
    assert cfg.pipelines[0].tables[0].target_name == 'stg_users'


def test_missing_file_raises(tmp_path: Path):
    with pytest.raises(ConfigError, match='Config file not found'):
        load_config(tmp_path / 'nope.yaml')


def test_missing_environment_raises(tmp_path: Path):
    path = tmp_path / 'mkpipe_project.yaml'
    path.write_text(yaml.safe_dump({'default_environment': 'prod', 'dev': {}}))

    with pytest.raises(ConfigError, match="Environment 'prod' not found"):
        load_config(path)
