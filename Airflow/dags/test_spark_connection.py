from datetime import datetime

from airflow.sdk import dag, Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="test_spark_connection",
    description="DAG in charge of testing spark connection.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
)
def test_connection():
        test = SparkSubmitOperator(
        task_id="test_spark_connection_task",
        application="/opt/airflow/files/spark/test.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        env_vars={
            "PYSPARK_PYTHON": "/home/airflow/.local/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/home/airflow/.local/bin/python3"
        }

    )


test_connection()
