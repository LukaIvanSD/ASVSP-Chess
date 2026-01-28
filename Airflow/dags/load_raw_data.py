from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook


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

        if hdfs_hook.check_for_path(hdfs_path=file_path):
            print(f"File {file_path} found in HDFS.")
        else:
            print(f"File {file_path} not found.")

    hdfs_file = load_data()
    check_hdfs_file(hdfs_file)


load_raw_data()
