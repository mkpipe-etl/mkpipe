import os
import time
from .config import load_config, get_config_value, TIMEZONE, ROOT_DIR, CONFIG_FILE
from .run_coordinators import get_coordinator

from .utils import Logger, InputTask, PipeSettings

os.environ['TZ'] = TIMEZONE
time.tzset()


# Function to manage priority levels and reset if necessary
def get_priority(pipeline_name, priority, custom_priority):
    """Adjust priority based on pipeline name or reset if necessary."""
    if custom_priority:
        return custom_priority
    priority -= 1
    return 200 if priority < 1 else priority


def main(config_file_name: str = None, pipeline_name_set=None, table_name_set=None):
    if not config_file_name:
        config_file_name = CONFIG_FILE
    logger = Logger(config_file_name)
    logger.info({'file_name': config_file_name})

    DATA = load_config(config_file=config_file_name)

    settings_values = get_config_value(['settings'], file_name=config_file_name)
    settings = PipeSettings(ROOT_DIR=str(ROOT_DIR), **settings_values)
    run_coordinator = get_config_value(['settings', 'run_coordinator'], file_name=config_file_name)

    # Validate that pipeline_name_set and table_name_set are sets
    if pipeline_name_set and not isinstance(pipeline_name_set, set):
        raise TypeError('pipeline_name_set must be a set')
    if table_name_set and not isinstance(table_name_set, set):
        raise TypeError('table_name_set must be a set')

    jobs = DATA['jobs']

    # Filter jobs based on pipeline_name_set if provided
    if pipeline_name_set:
        jobs = [job for job in jobs if job['name'] in pipeline_name_set]

    # Initialize the priority counter starting from 200
    priority = 200

    # Create a list to hold all tasks for the chord
    task_group = []

    # Iterate over the filtered jobs
    for job in jobs:
        pipeline_name = job['name']
        extract_task = job['extract_task']
        load_task = job['load_task']
        custom_priority = job.get('priority', None)

        logger.info({'message': f'Running pipeline: {pipeline_name}'})

        # Loader Configuration
        try:
            loader_conf = DATA['loaders'][load_task]['config']
            connection_params = DATA['connections'][loader_conf['connection_ref']]
            loader_variant = connection_params['variant']
            loader_conf['connection_params'] = connection_params

        except KeyError as e:
            logger.info(
                {
                    'error': f'Loader configuration issue: {str(e)}',
                    'loader_task': load_task,
                }
            )
            continue  # Skip this job if there is an error

        # Extractor Configuration
        try:
            extractor_conf = DATA['extractors'][extract_task]['config']
            connection_params = DATA['connections'][extractor_conf['connection_ref']]
            extractor_variant = connection_params['variant']
            extractor_conf['connection_params'] = connection_params
        except KeyError as e:
            logger.info(
                {
                    'error': f'Extractor configuration issue: {str(e)}',
                    'extract_task': extract_task,
                }
            )
            continue  # Skip this job if there is an error

        # Loop through each table in the extractor's configuration
        for table in extractor_conf.get('tables', []):
            # Skip this table if it's not in the provided set
            if table_name_set and table['name'] not in table_name_set:
                continue

            # Copy the extractor config for the current table
            table_extract_conf = extractor_conf.copy()
            table_extract_conf['table'] = table
            table_extract_conf.pop('tables', None)

            table_load_conf = loader_conf.copy()
            table_load_conf['table'] = table
            table_load_conf.pop('tables', None)

            # Get the adjusted priority level for the current pipeline
            priority = get_priority(pipeline_name, priority, custom_priority)

            task = InputTask(
                extractor_variant=extractor_variant,
                table_extract_conf=table_extract_conf,
                loader_variant=loader_variant,
                table_load_conf=table_load_conf,
                priority=custom_priority,
                settings=settings,
            )

            # Add the extraction task to the chord group, using kwargs
            task_group.append(task)

    if not task_group:
        logger.info({'warning': 'No tasks were scheduled to run.'})
        return

    coordinator = get_coordinator(run_coordinator)(task_group)
    coordinator.run()
