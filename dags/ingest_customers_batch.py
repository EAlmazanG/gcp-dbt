from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from utils.gcs_utils import download_and_upload_to_gcs_batch

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
