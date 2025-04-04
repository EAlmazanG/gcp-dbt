FROM python:3.10-slim

WORKDIR /app
COPY . /app

RUN apt-get update && apt-get install -y git && \
    pip install --upgrade pip && \
    pip install dbt-bigquery==1.9.1

ENV DBT_PROFILES_DIR=/app/profiles