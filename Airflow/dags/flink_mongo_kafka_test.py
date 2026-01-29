import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator as SimpleHttpOperator
from datetime import datetime

# --- PODEŠAVANJA ---
FLINK_HOST = "http://flink-jobmanager-1:8081"
JAR_PATH = "/opt/airflow/files/flink/flink-kafka-mongo-job-1.0.jar"
ENTRY_CLASS = "com.airflow.test.FlinkKafkaMongoJob"

def upload_jar_to_flink():
    """Funkcija šalje JAR na Flink i vraća njegov ID"""
    with open(JAR_PATH, 'rb') as f:
        files = {'jarfile': (os.path.basename(JAR_PATH), f, 'application/x-java-archive')}
        response = requests.post(f"{FLINK_HOST}/jars/upload", files=files)
        response.raise_for_status()

        # Flink vraća npr: {"filename": "/tmp/flink-web-xxx/5898..._fajl.jar", "status": "success"}
        filename = response.json()['filename']
        jar_id = filename.split('/')[-1]
        return jar_id

with DAG(
    dag_id="test_flink_kafka_mongo_integration",
    description="DAG in charge of testing Flink integration with Kafka and MongoDB.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
) as dag:

    # 1. Korak: Upload fajla i dobijanje JAR ID-a
    upload_task = PythonOperator(
        task_id='upload_jar',
        python_callable=upload_jar_to_flink
    )

    # 2. Korak: Pokretanje posla koristeći ID iz prethodnog koraka
    # Koristimo Jinja template {{ task_instance.xcom_pull(...) }} da uzmemo ID
    run_job = SimpleHttpOperator(
        task_id='run_flink_job',
        http_conn_id='flink_http_default', # Definiši ovo u Airflow Connections (HTTP tip)
        endpoint='/jars/{{ task_instance.xcom_pull(task_ids="upload_jar") }}/run',
        method='POST',
        data='{"entryClass": "' + ENTRY_CLASS + '", "parallelism": 1}',
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
    )

    upload_task >> run_job