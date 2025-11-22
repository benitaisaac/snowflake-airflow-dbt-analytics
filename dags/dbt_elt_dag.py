from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "you", "retries": 1, "retry_delay": timedelta(minutes=5)}

DBT_PROJECT_DIR = "/opt/airflow/dbt/stock_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT = "/home/airflow/dbtvenv/bin/dbt"  # use the venv dbt binary directly

with DAG(
    dag_id="dbt_elt_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,              # triggered by your ETL DAG
    catchup=False,
    tags=["dbt", "elt"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT} run"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT} test"
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT} snapshot"
    )

    dbt_run >> dbt_test >> dbt_snapshot
