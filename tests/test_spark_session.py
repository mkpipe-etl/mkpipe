from __future__ import annotations

import pytest

import pyspark
from mkpipe.models import SparkConfig
from mkpipe.plugins.jars import (
    SUPPORTED_SPARK_VERSIONS,
    collect_jars,
    comet_package,
    spark_artifact_suffix,
)
from mkpipe.spark.session import COMET_CONFS, _merge_comet_config


def test_spark_artifact_suffix():
    assert spark_artifact_suffix('4.1.3') == 'spark4.1_2.13'
    assert spark_artifact_suffix('4.0.0') == 'spark4.0_2.13'
    assert spark_artifact_suffix('3.5.1') == 'spark3.5_2.12'


def test_installed_pyspark_is_supported():
    major_minor = '.'.join(pyspark.__version__.split('.')[:2])

    assert major_minor in SUPPORTED_SPARK_VERSIONS


def test_comet_package_unsupported_spark_raises(monkeypatch):
    monkeypatch.setattr(pyspark, '__version__', '4.3.0')

    with pytest.raises(ValueError, match='No Comet 1.1.0 artifact'):
        comet_package()


def test_comet_package_matches_installed_pyspark():
    pkg = comet_package()

    assert pkg.startswith('org.apache.datafusion:comet-spark-spark')
    assert pkg.endswith(':1.1.0')


def test_merge_comet_config_without_existing_packages():
    merged = _merge_comet_config({}, '')

    assert merged['spark.jars.packages'] == comet_package()
    for key, value in COMET_CONFS.items():
        assert merged[key] == value


def test_merge_comet_config_appends_to_packages_param():
    merged = _merge_comet_config({}, 'org.foo:bar:1.0')

    assert merged['spark.jars.packages'] == f'org.foo:bar:1.0,{comet_package()}'


def test_merge_comet_config_extra_config_packages_win():
    merged = _merge_comet_config({'spark.jars.packages': 'org.foo:bar:1.0'}, 'org.baz:qux:2.0')

    assert merged['spark.jars.packages'] == f'org.foo:bar:1.0,{comet_package()}'


def test_merge_comet_config_user_values_win():
    merged = _merge_comet_config({'spark.plugins': 'custom.Plugin'}, '')

    assert merged['spark.plugins'] == 'custom.Plugin'
    assert merged['spark.shuffle.manager'] == COMET_CONFS['spark.shuffle.manager']


def test_merge_comet_config_local_jar_skips_maven():
    jars = '/opt/venv/lib/python3.13/site-packages/mkpipe/jars/comet-spark-spark4.1_2.13-1.1.0.jar'

    merged = _merge_comet_config({}, 'org.foo:bar:1.0', jars)

    assert merged['spark.jars.packages'] == 'org.foo:bar:1.0'
    assert 'comet-spark' not in merged['spark.jars.packages']


def test_merge_comet_config_local_jar_without_other_packages():
    # Regression: Docker images with all JARs pre-downloaded have no Maven
    # packages at all — the merged config must not require the
    # spark.jars.packages key (KeyError in the Comet log line).
    jars = '/opt/venv/lib/python3.13/site-packages/mkpipe/jars/comet-spark-spark4.1_2.13-1.1.0.jar'

    merged = _merge_comet_config({}, '', jars)

    assert 'spark.jars.packages' not in merged
    for key, value in COMET_CONFS.items():
        assert merged[key] == value


def test_merge_comet_config_user_comet_coordinate_wins():
    user_pkg = 'org.apache.datafusion:comet-spark-spark4.2_2.13:1.2.0'

    merged = _merge_comet_config({'spark.jars.packages': user_pkg}, '')

    assert merged['spark.jars.packages'] == user_pkg


def test_collect_jars_comet_flag_without_core_jars_dir():
    jars, packages = collect_jars(comet=True)

    assert all('comet' not in jar for jar in jars.split(',') if jar)
    assert comet_package() not in packages


def test_spark_config_comet_flag_defaults_to_false():
    cfg = SparkConfig()

    assert cfg.comet is False

    cfg = SparkConfig(comet=True)

    assert cfg.comet is True
