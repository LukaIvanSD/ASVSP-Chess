from datetime import datetime
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="silver_transformation_dag",
    description="DAG in charge of transforming bronze data to silver layer.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
)
def transform_silver_data_dag():

    HDFS_DEFAULT_FS = Variable.get("HDFS_DEFAULT_FS")
    command = f"""
        export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && /spark/bin/spark-submit --master spark://spark-master:7077 /home/jobs/silver_transformation.py {HDFS_DEFAULT_FS}/bronze_layer/bronze_transformed_data {HDFS_DEFAULT_FS}/silver_layer
    """

    prepare_silver_data = SSHOperator(
        task_id="prepare_silver_data",
        ssh_conn_id="SPARK_SSH",
        command=command,
        cmd_timeout=60 * 60,
    )

    trigger_gold_transformations = TriggerDagRunOperator(
        task_id="trigger_gold_transformations_dag",
        trigger_dag_id="gold_transformations_dag",
        wait_for_completion=False
    )

    prepare_silver_data >> trigger_gold_transformations

transform_silver_data_dag()