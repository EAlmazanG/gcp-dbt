from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

PROJECT_ID = "gcp-dbt-454911"
BUCKET = "gcp-dbt_datalake"

batch_sources = [
    {"name": "customers", "path": f"gs://{BUCKET}/raw/batch/raw_customers.csv"},
    {"name": "stores", "path": f"gs://{BUCKET}/raw/batch/raw_stores.csv"},
    {"name": "products", "path": f"gs://{BUCKET}/raw/batch/raw_products.csv"},
    {"name": "supplies", "path": f"gs://{BUCKET}/raw/batch/raw_supplies.csv"},
]

streaming_sources = [
    {"name": "items", "path": f"gs://{BUCKET}/raw/streaming/items/output/*"},
    {"name": "orders", "path": f"gs://{BUCKET}/raw/streaming/orders/output/*"},
]

default_args = {
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="sync_gcs_raw_to_bq",
    description="Load batch and streaming data from Cloud Storage to BigQuery",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sync", "raw", "bq"]
) as dag:

    start = EmptyOperator(task_id="start")

    for source in batch_sources:
        load_batch = BigQueryInsertJobOperator(
            task_id=f"load_batch_{source['name']}",
            configuration={
                "load": {
                    "sourceUris": [source["path"]],
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": "raw_batch",
                        "tableId": source["name"]
                    },
                    "sourceFormat": "CSV",
                    "skipLeadingRows": 1,
                    "writeDisposition": "WRITE_APPEND",
                    "autodetect": True,
                }
            }
        )

        start >> load_batch

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
                    "sourceFormat": "AUTO",
                    "writeDisposition": "WRITE_APPEND",
                }
            }
        )

        start >> load_stream