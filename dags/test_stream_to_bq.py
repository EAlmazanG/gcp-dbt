from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

PROJECT_ID = "gcp-dbt-454911"
BUCKET = "gcp-dbt_datalake"

streaming_sources = [
    {"name": "items", "path": f"gs://{BUCKET}/raw/streaming/items/output/*"},
]

default_args = {
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="test_stream_to_bq",
    description="Test streaming data from Cloud Storage to BigQuery",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sync", "raw", "bq", "test"]
) as dag:

    start = EmptyOperator(task_id="start")

    for source in streaming_sources:
        load_stream = BigQueryInsertJobOperator(
            task_id=f"load_stream_{source['name']}",
            configuration={
                "load": {
                    "sourceUris": [source["path"]],
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": "raw_streaming",
                        "tableId": source["name"]
                    },
                    "sourceFormat": "AVRO",
                    "writeDisposition": "WRITE_APPEND",
                    "autodetect": True
                }
            },
            gcp_conn_id="google_cloud_default"
        )

        start >> load_stream