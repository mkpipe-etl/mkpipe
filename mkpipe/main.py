import click
import os
from .run import main


@click.group(help='mkpipe CLI: A command-line interface for the mkpipe ETL framework.')
def cli():
    """CLI for mkpipe ETL framework."""
    pass


@cli.command(help='Run the mkpipe pipeline with the specified configuration file.')
@click.option(
    '--config',
    default=None,
    help='Path to the configuration file. Overrides MKPIPE_PROJECT_DIR.',
)
def run(config):
    """Run the mkpipe pipeline."""
    config_file = config or os.path.join(
        os.getenv('MKPIPE_PROJECT_DIR', '.'), 'mkpipe_project.yaml'
    )
    if not os.path.exists(config_file):
        click.echo(f'Error: Configuration file not found: {config_file}')
        click.echo('Set the MKPIPE_PROJECT_DIR environment variable or use the --config option.')
        return

    click.echo(f'Running mkpipe pipeline with configuration: {config_file}')
    os.chdir(os.path.dirname(config_file))
    main(config_file)


if __name__ == '__main__':
    cli()
