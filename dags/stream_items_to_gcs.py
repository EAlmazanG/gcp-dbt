from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from datetime import datetime
import os

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id="stream_items_to_gcs",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["streaming"],
    description="Run Dataflow to stream items from PubSub to GCS"
) as dag:

    run_dataflow = DataflowTemplatedJobStartOperator(
        task_id="run_dataflow_pubsub_to_gcs",
        project_id="gcp-dbt-454911",
        region="europe-southwest1",
        template="gs://dataflow-templates/latest/Streaming_Dataflow_Template",
        gcs_location="gs://europe-southwest1-gcp-dbt-c-0cae626e-bucket/scripts/beam/pubsub_to_gcs.py",
        job_name="stream-items-to-gcs-{{ ds_nodash }}",
        py_options=[],
        dataflow_default_options={
            "project": "gcp-dbt-454911",
            "region": "europe-southwest1",
            "tempLocation": "gs://gcp-dbt_datalake/temp/",
            "stagingLocation": "gs://gcp-dbt_datalake/staging/",
            "runner": "DataflowRunner",
            "streaming": "true"
        },
        options={
            "input_subscription": "projects/gcp-dbt-454911/subscriptions/items-stream-sub",
            "output_path": "gs://gcp-dbt_datalake/raw/streaming/items/",
        },
        py_requirements=['apache-beam[gcp]==2.48.0'],
        py_interpreter='python3',
        wait_until_finished=False,
    )
