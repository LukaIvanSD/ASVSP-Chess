from pyspark.sql import SparkSession, functions as sf
import sys

if __name__ == "__main__":
    ip_flows_dir = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("test_mongo_spark_connection").getOrCreate()

    ip_flows = spark.read.text(ip_flows_dir)

    ip_flows.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "test_collection").save()

    spark.stop()
