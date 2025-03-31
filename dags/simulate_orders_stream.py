from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.pubsub_utils import simulate_streaming_from_csv

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG("simulate_orders_stream",
         schedule_interval=None,
         catchup=False,
         default_args=default_args,
         tags=["streaming"],
         description="Simulate streaming of raw_orders.csv to Pub/Sub") as dag:

    simulate_task = PythonOperator(
        task_id="simulate_orders_stream",
        python_callable=simulate_streaming_from_csv,
        op_kwargs={
            "url": "https://raw.githubusercontent.com/EAlmazanG/gcp-dbt/2e54e79911728303a46fe4b78cedece0571efc0f/data/raw_orders.csv",
            "topic_path": "projects/gcp-dbt-454911/topics/orders-stream",
            "batch_size": 500,
            "interval_secs": 2
        }
    )