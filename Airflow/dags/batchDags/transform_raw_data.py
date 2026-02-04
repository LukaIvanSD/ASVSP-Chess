from datetime import datetime
from airflow.decorators import dag # Ispravljen import za Airflow 2.x
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="transform_dag",
    description="DAG in charge of transforming raw data.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
)
def transform_raw_data_dag():

    HDFS_DEFAULT_FS = Variable.get("HDFS_DEFAULT_FS")
    command = f"""
        export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && /spark/bin/spark-submit --master spark://spark-master:7077 /home/jobs/initial_transformation.py {HDFS_DEFAULT_FS}/raw_data/lichess.pgn {HDFS_DEFAULT_FS}/transformed_data
    """

    prepare_initial_data = SSHOperator(
        task_id="prepare_initial_data",
        ssh_conn_id="SPARK_SSH",
        command=command,
        cmd_timeout=60 * 60,
    )

    trigger_analytics = TriggerDagRunOperator(
        task_id="trigger_analytics_dag",
        trigger_dag_id="analytics_dag",
        wait_for_completion=False
    )

    prepare_initial_data >> trigger_analytics

transform_raw_data_dag()