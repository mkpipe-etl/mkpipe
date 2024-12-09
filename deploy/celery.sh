celery --app mkpipe.run_coordinators.coordinator_celery.app worker \
    --loglevel=info \
    --concurrency=4