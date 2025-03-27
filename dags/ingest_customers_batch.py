from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os

from google.cloud import storage

def download_and_upload_to_gcs_batch(url, local_path, bucket_name, blob_name):
    response = requests.get(url)
    with open(local_path, "wb") as f:
        f.write(response.content)
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG("ingest_customers_batch",
         schedule_interval=None,
         catchup=False,
         default_args=default_args,
         tags=["batch"],
         description="Ingest customers CSV from GitHub to GCS") as dag:

    ingest_task = PythonOperator(
        task_id="download_and_upload_to_gcs",
        python_callable=download_and_upload_to_gcs_batch,
        op_kwargs={
            "url": "https://raw.githubusercontent.com/EAlmazanG/gcp-dbt/c5db42465ce5364fe5464ad1b0dc9935aeae08c3/data/raw_customers.csv",
            "local_path": "/tmp/raw_customers.csv",
            "bucket_name": "gcp-dbt_datalake",
            "blob_name": "raw/batch/raw_customers.csv"
        },
    )
