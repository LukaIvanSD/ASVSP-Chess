from datetime import datetime
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="gold_transformations_dag",
    description="DAG in charge of transforming silver data to gold layer.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
)
def gold_transformations():

    HDFS_DEFAULT_FS = Variable.get("HDFS_DEFAULT_FS")
    MONGO_URI = Variable.get("MONGO_URI")

    base_spark_command = f"""
    export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && /spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,/opt/spark/jars/mongodb-driver-core-4.8.2.jar,/opt/spark/jars/bson-4.8.2.jar --conf "spark.driver.extraClassPath=/opt/spark/jars/*" --conf "spark.executor.extraClassPath=/opt/spark/jars/*"
    """

    tasks = [
        ("opening_popularity.py", "opening_popularity"),
        ("opening_above_average.py", "opening_above_average"),
        ("best_openings_by_elo.py", "best_openings_by_elo"),
        ("day_period_above_average.py", "day_period_above_average"),
        ("player_top_openings.py", "player_top_openings"),
        ("player_figure_analysis.py", "player_figure_analysis"),
        ("player_game_classification.py", "player_game_classification"),
        ("player_longest_streaks.py", "player_longest_streaks"),
        ("player_figure_bad_score_streak.py", "player_figure_bad_score_streak"),
        ("player_elo_gain_trend.py", "player_elo_gain_trend"),
    ]

    task_objects = []

    for script, collection in tasks:
        task = SSHOperator(
            task_id=f"gold_{collection}",
            ssh_conn_id="SPARK_SSH",
            command=f"""
            {base_spark_command} /spark/bin/spark-submit --jars /opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,/opt/spark/jars/mongodb-driver-core-4.8.2.jar,/opt/spark/jars/bson-4.8.2.jar --conf "spark.driver.extraClassPath=/opt/spark/jars/*" --conf "spark.executor.extraClassPath=/opt/spark/jars/*" /home/jobs/{script} {HDFS_DEFAULT_FS}/silver_layer {MONGO_URI} analytics_db
            """,
            cmd_timeout=60 * 60,
        )
        task_objects.append(task)

gold_transformations()