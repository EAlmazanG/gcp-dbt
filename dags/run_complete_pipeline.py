from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG("run_complete_pipeline",
         schedule_interval=None,
         catchup=False,
         default_args=default_args,
         tags=["orchestration"],
         description="Orquesta toda la pipeline de extremo a extremo") as dag:

    with TaskGroup("stream_simulation") as stream_simulation:
        simulate_items = TriggerDagRunOperator(
            task_id="trigger_simulate_items_stream",
            trigger_dag_id="simulate_items_stream",
            wait_for_completion=True,
            reset_dag_run=True
        )

        simulate_orders = TriggerDagRunOperator(
            task_id="trigger_simulate_orders_stream",
            trigger_dag_id="simulate_orders_stream",
            wait_for_completion=True,
            reset_dag_run=True
        )

    with TaskGroup("batch_ingest") as batch_ingest:
        ingest_customers = TriggerDagRunOperator(
            task_id="trigger_ingest_customers_batch",
            trigger_dag_id="ingest_customers_batch",
            wait_for_completion=True,
            reset_dag_run=True
        )

        ingest_products = TriggerDagRunOperator(
            task_id="trigger_ingest_products_batch",
            trigger_dag_id="ingest_products_batch",
            wait_for_completion=True,
            reset_dag_run=True
        )

        ingest_stores = TriggerDagRunOperator(
            task_id="trigger_ingest_stores_batch",
            trigger_dag_id="ingest_stores_batch",
            wait_for_completion=True,
            reset_dag_run=True
        )

        ingest_supplies = TriggerDagRunOperator(
            task_id="trigger_ingest_supplies_batch",
            trigger_dag_id="ingest_supplies_batch",
            wait_for_completion=True,
            reset_dag_run=True
        )

    sync_gcs = TriggerDagRunOperator(
        task_id="trigger_sync_gcs_raw_to_bq",
        trigger_dag_id="sync_gcs_raw_to_bq",
        wait_for_completion=True,
        reset_dag_run=True
    )

    run_dbt = TriggerDagRunOperator(
        task_id="trigger_launch_cloud_run_dbt_job",
        trigger_dag_id="launch_cloud_run_dbt_job",
        wait_for_completion=True,
        reset_dag_run=True
    )

    stream_simulation >> batch_ingest >> sync_gcs >> run_dbt
