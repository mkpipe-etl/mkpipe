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
@click.option(
    '--tags',
    default=None,
    help='Comma-separated tags to filter tables, e.g. --tags api,ingestion',
)
def run(config, pipeline, table, tags):
    from .api import run as api_run

    config_file = config or os.path.join(os.getcwd(), 'mkpipe_project.yaml')
    if not os.path.exists(config_file):
        click.echo(f'Error: Configuration file not found: {config_file}')
        click.echo('Use --config option or create mkpipe_project.yaml in current directory.')
        raise SystemExit(1)

    click.echo(f'Running mkpipe with config: {config_file}')
    tag_list = [t.strip() for t in tags.split(',')] if tags else None
    api_run(config=config_file, pipeline=pipeline, table=table, tags=tag_list)
    click.echo('Done.')


if __name__ == '__main__':
    cli()
