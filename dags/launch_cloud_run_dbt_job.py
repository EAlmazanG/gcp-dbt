from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG("launch_cloud_run_dbt_job",
         schedule_interval=None,
         catchup=False,
         default_args=default_args,
         tags=["dbt", "cloud_run"],
         description="Run dbt job on Cloud Run via gcloud CLI") as dag:

    run_dbt_job = BashOperator(
        task_id="trigger_dbt_job",
        bash_command=(
            "gcloud run jobs execute dbt-job "
            "--region=europe-southwest1 "
            "--project=gcp-dbt-454911 "
            "--wait"
        )
    )
