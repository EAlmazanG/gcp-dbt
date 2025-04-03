FROM python:3.10-slim

RUN pip install --upgrade pip
RUN pip install dbt-bigquery==1.9.3

WORKDIR /app
COPY . /app

ENV DBT_PROFILES_DIR=/app/profiles

CMD ["dbt", "run"]