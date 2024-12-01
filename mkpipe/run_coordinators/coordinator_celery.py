# celery --app mkpipe.run_coordinators.coordinator_celery.CoordinatorCelery.app worker --loglevel=info --concurrency=4
from celery import Celery, chord
from kombu import Queue
from dotenv import load_dotenv
import os
import datetime
from ..plugins import get_extractor, get_loader


class CoordinatorCelery:
    def __init__(self, task_group):
        self.task_group = task_group
        self.app = self.initialize_celery()
        self.register_tasks()

    def initialize_celery(self):
        # Load environment variables
        dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        load_dotenv(dotenv_path)

        # Celery app configuration
        broker_user = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
        broker_pass = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')
        broker_host = os.getenv('BROKER_HOST', 'rabbitmq')
        broker_port = os.getenv('BROKER_PORT', '5672')

        CELERY_BROKER_URL = (
            f'amqp://{broker_user}:{broker_pass}@{broker_host}:{broker_port}//'
        )
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
            broker_connection_retry_on_startup=True,
            worker_direct=True,
        )
        return app

    def register_tasks(self):
        """Register tasks dynamically after app initialization."""

        @self.app.task(
            bind=True,
            max_retries=3,
            retry_backoff=True,
            retry_backoff_jitter=True,
            track_started=True,
        )
        def extract_data(self_task, **kwargs):
            extractor_variant = kwargs['extractor_variant']
            current_table_conf = kwargs['current_table_conf']
            loader_variant = kwargs['loader_variant']
            loader_conf = kwargs['loader_conf']

            extractor = get_extractor(extractor_variant)(current_table_conf)
            data = extractor.extract()

            if data:
                self_task.request.app.send_task(
                    'load_data',
                    kwargs={
                        'loader_variant': loader_variant,
                        'loader_conf': loader_conf,
                        'data': data,
                    },
                    priority=201,
                    queue='mkpipe_queue',
                    exchange='mkpipe_exchange',
                    routing_key='mkpipe',
                )

            print('Extracted data successfully!')
            return True

        @self.app.task(
            bind=True,
            max_retries=3,
            retry_backoff=True,
            retry_backoff_jitter=True,
            track_started=True,
        )
        def load_data(self_task, **kwargs):
            loader_variant = kwargs['loader_variant']
            loader_conf = kwargs['loader_conf']
            data = kwargs['data']

            loader = get_loader(loader_variant)(loader_conf)
            elt_start_time = datetime.datetime.now()
            loader.load(data, elt_start_time)

            print('Loaded data successfully!')
            return True

        @self.app.task
        def on_all_tasks_completed(results):
            print(f'All tasks completed with results: {results}')

            if all(results):
                print('Both extraction and loading tasks succeeded.')
            else:
                print('One or more tasks failed. DBT not triggered.')

            return 'All tasks completed!' if all(results) else 'Some tasks failed!'

        # Assign tasks to the class instance
        self.extract_data = extract_data
        self.load_data = load_data
        self.on_all_tasks_completed = on_all_tasks_completed

    def run_parallel_tasks(self, task_group):
        chord(
            task_group,
            body=self.on_all_tasks_completed.s().set(
                queue='mkpipe_queue',
                exchange='mkpipe_exchange',
                routing_key='mkpipe',
            ),
        ).apply_async()

    def run(self):
        celery_task_group = []
        for task in self.task_group:
            celery_task_group.append(
                self.extract_data.s(
                    extractor_variant=task.extractor_variant,
                    current_table_conf=task.current_table_conf,
                    loader_variant=task.loader_variant,
                    loader_conf=task.loader_conf,
                ).set(
                    priority=task.priority,
                    queue='mkpipe_queue',
                    exchange='mkpipe_exchange',
                    routing_key='mkpipe',
                )
            )

        if celery_task_group:
            self.run_parallel_tasks(celery_task_group)
