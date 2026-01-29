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
def transform_raw_data():

    HDFS_DEFAULT_FS = Variable.get("HDFS_DEFAULT_FS")

    prepare_initial_data = SparkSubmitOperator(
        task_id="prepare_initial_data",
        application="/opt/airflow/files/spark/testic.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        application_args=[
            f"{HDFS_DEFAULT_FS}/raw_data/lichess_sample.pgn",
            f"{HDFS_DEFAULT_FS}/transformed_data_sample",
        ],
    )
    prepare_initial_data


transform_raw_data()
