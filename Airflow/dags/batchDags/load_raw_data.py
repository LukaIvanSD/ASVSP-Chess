from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="load_raw_data",
    description="DAG in charge of loading raw chess data to HDFS.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
)
def load_raw_data():

    @task
    def load_data():
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")
        local_source = "files/chessData/lichess.pgn"
        hdfs_destination = "/raw_data/lichess.pgn"
        hdfs_hook.load_file(
            source=local_source, destination=hdfs_destination, overwrite=True
        )
        return hdfs_destination

    @task
    def check_hdfs_file(file_path):
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")
        if not hdfs_hook.check_for_path(hdfs_path=file_path):
            raise ValueError(f"File {file_path} not found in HDFS.")
        print(f"File {file_path} found in HDFS.")

    loaded_file = load_data()
    checked_file = check_hdfs_file(loaded_file)

    trigger_bronze_transform = TriggerDagRunOperator(
        task_id="load_real_time_data",
        trigger_dag_id="bronze_transformation_dag",
        wait_for_completion=False
    )

    checked_file >> trigger_bronze_transform

load_raw_data()
