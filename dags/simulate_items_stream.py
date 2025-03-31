from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.pubsub_utils import simulate_streaming_from_csv

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG("simulate_items_stream",
         schedule_interval=None,
         catchup=False,
         default_args=default_args,
         tags=["streaming"],
         description="Simulate streaming of raw_items.csv to Pub/Sub") as dag:

    simulate_task = PythonOperator(
        task_id="simulate_items_stream",
        python_callable=simulate_streaming_from_csv,
        op_kwargs={
            "url": "https://raw.githubusercontent.com/EAlmazanG/gcp-dbt/bcdf04810ef936ffc3cbfa452b112b45bcf29b37/data/raw_items.csv",
            "topic_path": "projects/gcp-dbt-454911/topics/items-stream",
            "batch_size": 1000,
            "interval_secs": 1
        }
    )