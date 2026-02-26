from datetime import datetime
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="bronze_transformation_dag",
    description="DAG in charge of transforming raw data to bronze layer.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
)
def transform_raw_data_dag():

    HDFS_DEFAULT_FS = Variable.get("HDFS_DEFAULT_FS")
    command = f"""
        export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && /spark/bin/spark-submit --master spark://spark-master:7077 /home/jobs/bronze_transformation.py {HDFS_DEFAULT_FS}/raw_data/lichess.pgn {HDFS_DEFAULT_FS}/bronze_layer
    """

    prepare_initial_data = SSHOperator(
        task_id="prepare_initial_data",
        ssh_conn_id="SPARK_SSH",
        command=command,
        cmd_timeout=60 * 60,
    )

    trigger_silver_transformation = TriggerDagRunOperator(
        task_id="trigger_silver_transformation_dag",
        trigger_dag_id="silver_transformation_dag",
        wait_for_completion=False
    )

    prepare_initial_data >> trigger_silver_transformation

transform_raw_data_dag()