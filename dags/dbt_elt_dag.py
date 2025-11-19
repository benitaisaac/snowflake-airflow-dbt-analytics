from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "you", "retries": 1, "retry_delay": timedelta(minutes=5)}

DBT_DIR = "/opt/airflow/dbt/stock_analytics"
DBT = "/home/airflow/.local/bin/dbt"  

with DAG(
    dag_id="dbt_elt_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "elt"],
) as dag:

    run_dbt = BashOperator(
    task_id="run_dbt_models",
    bash_command=(
        "source /home/airflow/dbtvenv/bin/activate && "
        "cd /opt/airflow/dbt/stock_analytics && "
        "dbt run --profiles-dir /opt/airflow/dbt"
    ),
)

    test_dbt = BashOperator(
        task_id="test_dbt_models",
        bash_command=(
            "source /home/airflow/dbtvenv/bin/activate && "
            "cd /opt/airflow/dbt/stock_analytics && "
            "dbt test --profiles-dir /opt/airflow/dbt"
        ),
    )

    run_dbt >> test_dbt
