from celery import Celery, chord
from kombu import Queue
from .plugins import get_extractor, get_loader
from dotenv import load_dotenv
import os
import datetime

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

# Celery app configuration
broker_user = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
broker_pass = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')
broker_host = os.getenv('BROKER_HOST', 'rabbitmq')
broker_port = os.getenv('BROKER_PORT', '5672')

CELERY_BROKER_URL = f'amqp://{broker_user}:{broker_pass}@{broker_host}:{broker_port}//'
CELERY_BACKEND_URL = os.getenv('CELERY_BACKEND_URL', 'rpc://')

app = Celery('celery_app')
app.conf.update(
    broker_url=CELERY_BROKER_URL,
    result_backend=CELERY_BACKEND_URL,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_queues=(
        Queue(
            'mkpipe_queue',
            exchange='mkpipe_exchange',
            routing_key='mkpipe',
            queue_arguments={'x-max-priority': 255},
        ),
    ),
    task_routes={
        'elt.celery_app.extract_data': {'queue': 'mkpipe_queue'},
        'elt.celery_app.load_data': {'queue': 'mkpipe_queue'},
    },
    task_default_queue='mkpipe_queue',
    task_default_exchange='mkpipe_exchange',
    task_default_routing_key='mkpipe',
    # Retry settings
    task_retry_limit=3,  # Maximum retries
    task_retry_backoff=True,  # Enable exponential backoff
    task_retry_backoff_jitter=True,  # Adds slight randomness
    result_expires=3600,  # Result expiration time in seconds
    result_chord_retry_interval=60,
    broker_connection_retry_on_startup=True,  # Enable connection retries at startup
    worker_direct=True,
)


@app.task(
    bind=True,  # Bind self to access retry
    # autoretry_for=(ConnectionError, TimeoutError,),  # Automatically retry on these exceptions
    # autoretry_for=(Exception,),
    max_retries=3,  # Maximum of 3 retries
    retry_backoff=True,  # Exponential backoff (e.g., 1, 2, 4 seconds)
    retry_backoff_jitter=True,  # Adds slight randomness to backoff timing
    track_started=True,
)
def load_data(self, **kwargs):
    """
    Load data using the provided loader configuration.

    :param self: The task instance (used for retrying the task)
    :param kwargs: Dictionary containing loader_variant, loader_conf, and data
    """
    # Extract the necessary parameters from kwargs
    loader_variant = kwargs['loader_variant']
    loader_conf = kwargs['loader_conf']
    data = kwargs['data']

    # Initialize loader instance
    loader = get_loader(loader_variant)(loader_conf)

    # Record the start time of the loading process
    elt_start_time = datetime.datetime.now()

    # Load the extracted data with the start time
    loader.load(data, elt_start_time)

    print('Loaded data successfully!')
    return True  # Return True to indicate success


@app.task(
    bind=True,
    # autoretry_for=(Exception,),
    max_retries=3,  # Maximum of 3 retries
    retry_backoff=True,  # Exponential backoff (e.g., 1, 2, 4 seconds)
    retry_backoff_jitter=True,  # Adds slight randomness to backoff timing
    track_started=True,
)
def extract_data(self, **kwargs):
    """
    Extract data using the provided extractor configuration and load it using the loader configuration.

    :param self: The task instance (used for retrying the task)
    :param kwargs: Dictionary containing extractor_variant, current_table_conf, loader_variant, and loader_conf
    """
    # Extract the necessary parameters from kwargs
    extractor_variant = kwargs['extractor_variant']
    current_table_conf = kwargs['current_table_conf']
    loader_variant = kwargs['loader_variant']
    loader_conf = kwargs['loader_conf']

    # Initialize extractor and loader instances
    extractor = get_extractor(extractor_variant)(current_table_conf)

    # Perform the data extraction
    data = extractor.extract()

    if data:
        # Schedule the data loading as a separate task
        load_data.apply_async(
            kwargs={
                'loader_variant': loader_variant,
                'loader_conf': loader_conf,
                'data': data,
            },
            priority=201,  # higher number is have much priority 255 is max
            queue='mkpipe_queue',  # Ensure it uses the priority queue
            exchange='mkpipe_exchange',
            routing_key='mkpipe',
        )

    print('Extracted data successfully!')
    return True  # Return True to indicate success


@app.task
def on_all_tasks_completed(results):
    # This will now receive the results from both extract and load tasks
    print(f'All tasks completed with results: {results}')

    # Check if all tasks were successful before triggering dbt
    if all(results):
        print('Both extraction and loading tasks succeeded.')
    else:
        print('One or more tasks failed. DBT not triggered.')

    return 'All tasks completed!' if all(results) else 'Some tasks failed!'


def run_parallel_tasks(task_group):
    # Define the chord with the modified task group and success callback
    chord(
        task_group,
        body=on_all_tasks_completed.s().set(
            queue='mkpipe_queue',
            exchange='mkpipe_exchange',
            routing_key='mkpipe',
        ),
    ).apply_async()


