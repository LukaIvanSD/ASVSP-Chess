import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator as SimpleHttpOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

FLINK_HOST = "http://flink-jobmanager-1:8081"
JAR_PATH = "/opt/airflow/files/flink/transform-finished-games-job-1.0.jar"
ENTRY_CLASS = "com.flink.streaming.TransformFinishedGamesJob"

def upload_jar_to_flink():
    """Funkcija šalje JAR na Flink i vraća njegov ID"""
    with open(JAR_PATH, 'rb') as f:
        files = {'jarfile': (os.path.basename(JAR_PATH), f, 'application/x-java-archive')}
        response = requests.post(f"{FLINK_HOST}/jars/upload", files=files)
        response.raise_for_status()

        filename = response.json()['filename']
        jar_id = filename.split('/')[-1]
        return jar_id

with DAG(
    dag_id="flink_transform_finished_games",
    description="DAG in charge of transforming finished games data using Flink.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
) as dag:

    upload_task = PythonOperator(
        task_id='upload_transformation_finished_games_jar',
        python_callable=upload_jar_to_flink
    )

    run_job = SimpleHttpOperator(
        task_id='run_flink_transformation_finished_games_job',
        http_conn_id='flink_http_default',
        endpoint='/jars/{{ task_instance.xcom_pull(task_ids="upload_transformation_finished_games_jar") }}/run',
        method='POST',
        data='{"entryClass": "' + ENTRY_CLASS + '", "parallelism": 1}',
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
    )

    trigger_process_ongoing_game_move = TriggerDagRunOperator(
        task_id="trigger_load_finished_games_data_dag",
        trigger_dag_id="flink_process_ongoing_game_move",
        wait_for_completion=False
    )

    upload_task >> run_job >> trigger_process_ongoing_game_move