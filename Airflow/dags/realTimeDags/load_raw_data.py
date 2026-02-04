from datetime import datetime

from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id="load_real_time_raw_data",
    description="DAG in charge of getting real time data.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
)
def load_raw_data():

    load_data = BashOperator(
        task_id="load_real_time_data",
        bash_command="python3 /opt/airflow/files/realTime/fetch_real_time_data.py"
    )


load_raw_data()

