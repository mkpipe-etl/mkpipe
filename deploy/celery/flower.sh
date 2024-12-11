#!/bin/sh

# Construct the broker and broker API URLs
RABBITMQ_DEFAULT_USER=${RABBITMQ_DEFAULT_USER:-guest}
RABBITMQ_DEFAULT_PASS=${RABBITMQ_DEFAULT_PASS:-guest}
RABBITMQ_HOST=${RABBITMQ_HOST:-localhost}
RABBITMQ_MANAGEMENT_PORT=${RABBITMQ_MANAGEMENT_PORT:-15672}
BROKER_URL="amqp://${RABBITMQ_DEFAULT_USER}:${RABBITMQ_DEFAULT_PASS}@${RABBITMQ_HOST}:5672//"
BROKER_API_URL="http://${RABBITMQ_DEFAULT_USER}:${RABBITMQ_DEFAULT_PASS}@${RABBITMQ_HOST}:${RABBITMQ_MANAGEMENT_PORT}/api/"

# Debugging info
echo "Broker URL: ${BROKER_URL}"
echo "Broker API URL: ${BROKER_API_URL}"

# Start Flower
celery --app mkpipe.run_coordinators.coordinator_celery.app flower \
    --port=5555 \
    --broker=${BROKER_URL} \
    --broker_api=${BROKER_API_URL} \
    --url_prefix=flower
