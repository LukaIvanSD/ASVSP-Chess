from datetime import datetime
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator

@dag(
    dag_id="analytics_dag",
    description="DAG in charge of running analytical queries.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
)
def analytics_dag():
    MONGO_URI = Variable.get("MONGO_URI")
    HDFS_DEFAULT_FS = Variable.get("HDFS_DEFAULT_FS")
    command = f"""
    export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && /spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,/opt/spark/jars/mongodb-driver-core-4.8.2.jar,/opt/spark/jars/bson-4.8.2.jar --conf "spark.driver.extraClassPath=/opt/spark/jars/*" --conf "spark.executor.extraClassPath=/opt/spark/jars/*" /home/jobs/initial_analytics.py {HDFS_DEFAULT_FS}/initial_transformed_data/ {MONGO_URI} analytics_db
    """


    prepare_initial_data = SSHOperator(
        task_id="prepare_initial_data",
        ssh_conn_id="SPARK_SSH",
        command=command,
        cmd_timeout=60 * 60,
    )

analytics_dag()