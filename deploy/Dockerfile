# Use a base image
ARG PYTHON_VERSION=3.11.11
ARG IMAGE_VARIANT=slim

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3

# Set working directory
WORKDIR /app

# Install necessary dependencies (including Java OpenJDK, cron, and procps)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    bash \
    cron \
    procps \
    libsnappy-java \
    libsnappy1v5 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install mkpipe components
RUN pip install --upgrade pip && \
    pip install mkpipe-extractor-postgres mkpipe-loader-postgres flower --no-cache-dir

# Copy the remaining files into the image
COPY celery.sh /app/celery.sh
COPY flower.sh /app/flower.sh

# Set environment variable for mkpipe project directory
ENV MKPIPE_PROJECT_DIR=/app

# Set the Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Ensure /var/log/cron.log exists
RUN touch /var/log/cron.log

# Entrypoint to start cron and keep the container running
# CMD service cron start && tail -f /var/log/cron.log || tail -f /dev/null
