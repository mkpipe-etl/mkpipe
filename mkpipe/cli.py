import os

import click


@click.group(help='mkpipe CLI: Spark-based modular ETL pipeline framework.')
def cli():
    pass


@cli.command(help='Run mkpipe pipelines from a configuration file.')
@click.option(
    '--config', '-c',
    default=None,
    help='Path to the configuration file. Defaults to mkpipe_project.yaml in current dir.',
)
@click.option(
    '--pipeline', '-p',
    default=None,
    help='Run only the specified pipeline.',
)
@click.option(
    '--table', '-t',
    default=None,
    help='Run only the specified table.',
)
def run(config, pipeline, table):
    from .api import run as api_run

    config_file = config or os.path.join(os.getcwd(), 'mkpipe_project.yaml')
    if not os.path.exists(config_file):
        click.echo(f'Error: Configuration file not found: {config_file}')
        click.echo('Use --config option or create mkpipe_project.yaml in current directory.')
        raise SystemExit(1)

    click.echo(f'Running mkpipe with config: {config_file}')
    api_run(config=config_file, pipeline=pipeline, table=table)
    click.echo('Done.')


if __name__ == '__main__':
    cli()
