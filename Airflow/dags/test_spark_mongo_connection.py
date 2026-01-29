from datetime import datetime

from airflow.sdk import dag, Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="analytical_queries",
    description="DAG in charge of running queries on top of transformed data.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
)
def test_mongo_spark():

    HDFS_DEFAULT_FS = Variable.get("HDFS_DEFAULT_FS")
    MONGO_URI = Variable.get("MONGO_URI")

    test = SparkSubmitOperator(
        task_id="test_mongo_spark_connection",
        application="/opt/airflow/files/spark/test_mongo_spark_connection.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf={
            "spark.jars": "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
            "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
            "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
            "/opt/spark/jars/bson-4.8.2.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
        },
        application_args=[
            f"{HDFS_DEFAULT_FS}/raw_data/lichess_sample.pgn",
            MONGO_URI,
            "test_db",
        ],
    )
    test


test_mongo_spark()
