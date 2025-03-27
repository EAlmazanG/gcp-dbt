from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG("update_all_data",
         schedule_interval=None,
         catchup=False,
         default_args=default_args,
         tags=["orchestrator"],
         description="Trigger all batch DAGs") as dag:

    trigger_customers = TriggerDagRunOperator(
        task_id="trigger_customers",
        trigger_dag_id="ingest_customers_batch"
    )

    trigger_products = TriggerDagRunOperator(
        task_id="trigger_products",
        trigger_dag_id="ingest_products_batch"
    )

    trigger_orders = TriggerDagRunOperator(
        task_id="trigger_orders",
        trigger_dag_id="ingest_orders_batch"
    )

    trigger_payments = TriggerDagRunOperator(
        task_id="trigger_payments",
        trigger_dag_id="ingest_payments_batch"
    )