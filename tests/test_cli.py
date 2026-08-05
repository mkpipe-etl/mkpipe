from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from mkpipe.cli import cli


def test_run_missing_config_exits_with_error(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ['run', '--config', str(tmp_path / 'nope.yaml')])

    assert result.exit_code == 1
    assert 'Configuration file not found' in result.output


def test_run_defaults_to_project_yaml_in_cwd(tmp_path: Path) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
        Path(cwd, 'mkpipe_project.yaml').write_text('settings: {}\n')
        with patch('mkpipe.api.run') as api_run:
            result = runner.invoke(cli, ['run'])

    assert result.exit_code == 0
    api_run.assert_called_once()
    assert api_run.call_args.kwargs['config'].endswith('mkpipe_project.yaml')


def test_run_passes_filters_and_parses_tags(tmp_path: Path) -> None:
    config_file = tmp_path / 'project.yaml'
    config_file.write_text('settings: {}\n')

    runner = CliRunner()
    with patch('mkpipe.api.run') as api_run:
        result = runner.invoke(
            cli,
            [
                'run',
                '--config',
                str(config_file),
                '--pipeline',
                'pg_to_iceberg',
                '--table',
                'apld_bill_rt_tax',
                '--tags',
                'api, ingestion ',
            ],
        )

    assert result.exit_code == 0
    api_run.assert_called_once_with(
        config=str(config_file),
        pipeline='pg_to_iceberg',
        table='apld_bill_rt_tax',
        tags=['api', 'ingestion'],
    )


def test_run_without_tags_passes_none(tmp_path: Path) -> None:
    config_file = tmp_path / 'project.yaml'
    config_file.write_text('settings: {}\n')

    runner = CliRunner()
    with patch('mkpipe.api.run') as api_run:
        result = runner.invoke(cli, ['run', '--config', str(config_file)])

    assert result.exit_code == 0
    assert api_run.call_args.kwargs['tags'] is None


def test_install_jars_invokes_download() -> None:
    runner = CliRunner()
    with patch('mkpipe.plugins.jars.download_jars') as download_jars:
        result = runner.invoke(cli, ['install-jars'])

    assert result.exit_code == 0
    download_jars.assert_called_once_with()
