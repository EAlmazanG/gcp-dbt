FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y git && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir dbt-bigquery==1.9.3

ENV DBT_PROFILES_DIR=/app/profiles

CMD ["dbt", "run"]
