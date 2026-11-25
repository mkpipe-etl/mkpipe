import pytest

from mkpipe import api
from mkpipe.models import (
    ConnectionConfig,
    ExtractResult,
    MkpipeConfig,
    PipelineConfig,
    SettingsConfig,
    TableConfig,
)
from mkpipe.plugins import registry


class _Backend:
    def get_table_status(self, pipeline_name, table_name):
        return None

    def get_last_point(self, pipeline_name, table_name):
        return None

    def manifest_table_update(self, **kwargs):
        pass


class _DataFrame:
    class _Schema:
        fields = []

    schema = _Schema()


class _Extractor:
    def __init__(self, connection):
        pass

    def extract(self, table, spark, last_point=None):
        return ExtractResult(df=_DataFrame())


class _Loader:
    observed_settings = []

    def __init__(self, connection):
        self.ignore_extra_source_columns = False

    def load(self, table, data, spark):
        _Loader.observed_settings.append(table.ignore_extra_source_columns)


@pytest.mark.parametrize('entrypoint', ['run', 'load'])
@pytest.mark.parametrize(
    ('global_value', 'pipeline_value', 'table_value', 'expected'),
    [
        (True, None, None, True),
        (False, True, None, True),
        (True, False, None, False),
        (True, True, False, False),
        (False, False, True, True),
    ],
)
def test_ignore_extra_source_columns_precedence(
    monkeypatch, entrypoint, global_value, pipeline_value, table_value, expected
):
    _Loader.observed_settings = []
    config = MkpipeConfig(
        settings=SettingsConfig(ignore_extra_source_columns=global_value),
        connections={
            'src': ConnectionConfig(variant='src'),
            'dst': ConnectionConfig(variant='dst'),
        },
        pipelines=[
            PipelineConfig(
                name='pipeline',
                source='src',
                destination='dst',
                ignore_extra_source_columns=pipeline_value,
                tables=[
                    TableConfig(
                        name='src', target_name='dst', ignore_extra_source_columns=table_value
                    )
                ],
            )
        ],
    )
    monkeypatch.setattr(api, '_ensure_spark', lambda spark, cfg: object())
    monkeypatch.setattr(api, '_create_backend', lambda cfg: _Backend())
    monkeypatch.setattr(registry, 'get_extractor', lambda variant: _Extractor)
    monkeypatch.setattr(registry, 'get_loader', lambda variant: _Loader)

    if entrypoint == 'run':
        api.run(config)
    else:
        api.load(config, 'dst', df=_DataFrame())

    assert _Loader.observed_settings == [expected]
