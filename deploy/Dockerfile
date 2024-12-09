# Use a base image
ARG PYTHON_VERSION=3.11.11
ARG IMAGE_VARIANT=slim

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3

# Set working directory
WORKDIR /app

# Install necessary dependencies (including Java OpenJDK)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    bash \
    libsnappy-java \
    libsnappy1v5 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install mkpipe components
RUN pip install --upgrade pip && \
    pip install mkpipe-extractor-postgres mkpipe-loader-postgres flower --no-cache-dir

# Copy the remaining files into the image
COPY .env mkpipe_project.yaml celery.sh flower.sh test_mkpipe.py /app

# Set environment variable for mkpipe project directory
ENV MKPIPE_PROJECT_DIR=/app

# Set the Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
