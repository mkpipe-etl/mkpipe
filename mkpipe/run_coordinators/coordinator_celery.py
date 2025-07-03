# celery --app mkpipe.run_coordinators.coordinator_celery.app worker --loglevel=info --concurrency=4
import datetime
from urllib.parse import quote_plus

from celery import Celery, chord
from kombu import Queue

from ..config import CONFIG_FILE, get_config_value
from ..plugins import get_extractor, get_loader
from ..utils import Logger


# Determine whether to initialize Celery based on the run_coordinator value
run_coordinator = get_config_value(['settings', 'run_coordinator'], file_name=CONFIG_FILE)

if run_coordinator == 'celery':
    logger = Logger(__file__)
    # Celery app configuration
    broker_type = get_config_value(['settings', 'broker', 'broker_type'], file_name=CONFIG_FILE)
    broker_host = get_config_value(['settings', 'broker', 'host'], file_name=CONFIG_FILE)
    broker_port = get_config_value(['settings', 'broker', 'port'], file_name=CONFIG_FILE)
    broker_user = get_config_value(['settings', 'broker', 'user'], file_name=CONFIG_FILE)
    broker_password = get_config_value(['settings', 'broker', 'password'], file_name=CONFIG_FILE)

    CELERY_BROKER_URL = (
        f'amqp://{broker_user}:{quote_plus(str(broker_password))}@{broker_host}:{broker_port}//'
    )

    backend_type = get_config_value(['settings', 'backend', 'variant'], file_name=CONFIG_FILE)
    backend_host = get_config_value(['settings', 'backend', 'host'], file_name=CONFIG_FILE)
    backend_port = get_config_value(['settings', 'backend', 'port'], file_name=CONFIG_FILE)
    backend_user = get_config_value(['settings', 'backend', 'user'], file_name=CONFIG_FILE)
    backend_password = get_config_value(['settings', 'backend', 'password'], file_name=CONFIG_FILE)
    backend_database = get_config_value(['settings', 'backend', 'database'], file_name=CONFIG_FILE)

    CELERY_BACKEND_URL = f'db+{backend_type}://{backend_user}:{quote_plus(str(backend_password))}@{backend_host}:{backend_port}/{backend_database}'

    # print("CELERY_BROKER_URL:", CELERY_BROKER_URL)
    # print("CELERY_BACKEND_URL:",CELERY_BACKEND_URL)

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
        task_retry_limit=3,
        task_retry_backoff=True,
        task_retry_backoff_jitter=True,
        result_expires=24*3600,
        result_chord_retry_interval=60,
        broker_connection_retry_on_startup=True,
        worker_direct=True,
    )

    """Register tasks dynamically for the Celery app."""

    @app.task(
        bind=True,
        max_retries=3,
        retry_backoff=True,
        retry_backoff_jitter=True,
        track_started=True,
    )
    def extract_data(self_task, **kwargs):
        extractor_variant = kwargs['extractor_variant']
        table_extract_conf = kwargs['table_extract_conf']
        loader_variant = kwargs['loader_variant']
        table_load_conf = kwargs['table_load_conf']
        settings = kwargs['settings']

        extractor = get_extractor(extractor_variant)(table_extract_conf, settings)
        data = extractor.extract()

        if data:
            load_data.apply_async(
                kwargs={
                    'loader_variant': loader_variant,
                    'table_load_conf': table_load_conf,
                    'data': data,
                    'settings': settings,
                },
                priority=201,
                queue='mkpipe_queue',
                exchange='mkpipe_exchange',
                routing_key='mkpipe',
            )

        logger.info({'message': 'Extracted data successfully!'})
        return True

    @app.task(
        bind=True,
        max_retries=3,
        retry_backoff=True,
        retry_backoff_jitter=True,
        track_started=True,
    )
    def load_data(self_task, **kwargs):
        loader_variant = kwargs['loader_variant']
        table_load_conf = kwargs['table_load_conf']
        settings = kwargs['settings']
        data = kwargs['data']

        loader = get_loader(loader_variant)(table_load_conf, settings)
        elt_start_time = datetime.datetime.now()
        loader.load(data, elt_start_time)

        logger.info({'message': 'Loaded data successfully!'})
        return True

    @app.task
    def on_all_tasks_completed(results):
        logger.info({'message': f'All tasks completed with results: {results}'})

        if all(results):
            logger.info({'message': 'Both extraction and loading tasks succeeded.'})
        else:
            logger.warning({'message': 'One or more tasks failed. DBT not triggered.'})

        return 'All tasks completed!' if all(results) else 'Some tasks failed!'

    def run_parallel_tasks(task_group):
        chord(
            task_group,
            body=on_all_tasks_completed.s().set(
                queue='mkpipe_queue',
                exchange='mkpipe_exchange',
                routing_key='mkpipe',
            ),
        ).apply_async()


else:
    app = None


class CoordinatorCelery:
    def __init__(self, task_group):
        self.logger = Logger(__file__)
        self.task_group = task_group

    def run(self):
        if not app:
            raise RuntimeError('Celery app is not initialized')

        celery_task_group = []
        for task in self.task_group:
            celery_task_group.append(
                extract_data.s(
                    extractor_variant=task.extractor_variant,
                    table_extract_conf=task.table_extract_conf,
                    loader_variant=task.loader_variant,
                    table_load_conf=task.table_load_conf,
                    settings=task.settings.dict(),
                ).set(
                    priority=task.priority,
                    queue='mkpipe_queue',
                    exchange='mkpipe_exchange',
                    routing_key='mkpipe',
                )
            )

        if celery_task_group:
            run_parallel_tasks(celery_task_group)
            self.logger.info({'message': 'Tasks have been sended to the Celery Queue!'})
