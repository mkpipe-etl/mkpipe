# Use a base image
ARG PYTHON_VERSION=3.11.11
ARG IMAGE_VARIANT=slim

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3

# Set working directory
ENV APP_HOME /app
WORKDIR ${APP_HOME}
ENV MKPIPE_PROJECT_DIR ${APP_HOME}

# Install necessary dependencies (including Java OpenJDK, cron, and procps)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        bash \
        cron \
        procps \
        libsqlite3-dev \
        build-essential \
        libsnappy-java \
        libsnappy1v5 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy the remaining files into the image
COPY requirements.txt requirements.txt

# Detect JAVA_HOME dynamically and create a standard symlink
RUN actual_java_home=$(dirname $(dirname $(readlink -f $(which java))) ) && \
    ln -s "$actual_java_home" /usr/lib/jvm/java-17-openjdk && \
    echo "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk" >> /etc/profile.d/java_home.sh && \
    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /etc/profile.d/java_home.sh

# Set ENV for Docker build-time and runtime
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH $PATH:$JAVA_HOME/bin

# Ensure /var/log/cron.log exists
RUN touch /var/log/cron.log

RUN pip install --upgrade pip && \
    pip install -r requirements.txt --no-cache-dir
    
# Entrypoint to start cron and keep the container running
# CMD service cron start && tail -f /var/log/cron.log || tail -f /dev/null
