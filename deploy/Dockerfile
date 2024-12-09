# Dockerfile
FROM python:3.13.1-alpine

# Set working directory
WORKDIR /app

# Install mkpipe components
RUN pip install mkpipe-extractor-postgres mkpipe-loader-postgres flower

# Copy the remaining files into the image
COPY .env mkpipe_project.yaml celery.sh flower.sh test_mkpipe.py /app

# Set environment variable for mkpipe project directory
ENV MKPIPE_PROJECT_DIR=/app
